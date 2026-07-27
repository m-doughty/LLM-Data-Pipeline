[![Actions Status](https://github.com/m-doughty/LLM-Data-Pipeline/actions/workflows/test.yml/badge.svg)](https://github.com/m-doughty/LLM-Data-Pipeline/actions)

NAME
====

LLM::Data::Pipeline - Generic pipeline framework for sequential data processing with checkpointing

SYNOPSIS
========

```raku
use LLM::Data::Pipeline;
use LLM::Data::Pipeline::Context;
use LLM::Data::Pipeline::Step;
use LLM::Data::Pipeline::Plan;
use LLM::Data::Pipeline::Runner;

class MyStep does LLM::Data::Pipeline::Step {
    method name(--> Str:D) { 'my-step' }
    method description(--> Str:D) { 'Does something useful' }
    method requires(--> List) { ('input',) }
    method optional(--> List) { ('config',) }
    method provides(--> List) { ('output',) }
    method execute(LLM::Data::Pipeline::Context:D $ctx --> Nil) {
        my $input = $ctx.get('input');
        $ctx.set('output', "processed: $input");
    }
}

my LLM::Data::Pipeline::Plan $plan .= new;
$plan.add-step(MyStep.new);

my LLM::Data::Pipeline::Context $ctx .= new;
$ctx.set('input', 'hello');

my LLM::Data::Pipeline::Runner $runner .= new(
    on-event => -> LLM::Data::Pipeline::Event $e {
        say "{$e.seq}\t{$e.kind}";
    }
);
$runner.run($plan, $ctx, :checkpoint-path('checkpoint.json'.IO));

say $ctx.get('output');  # "processed: hello"

# Resume from checkpoint (completed steps arrive as step-skipped events)
my $ctx2 = $runner.resume($plan, 'checkpoint.json'.IO);
```

DESCRIPTION
===========

LLM::Data::Pipeline provides a domain-agnostic framework for building sequential data processing pipelines with automatic checkpointing and resume capability.

LLM::Data::Pipeline::Step (Role)
--------------------------------

Any class can be a pipeline step by doing this role.

```raku
method name(--> Str:D) { ... }           # Unique step identifier
method description(--> Str:D) { ... }    # Human-readable description
method requires(--> List) { () }         # Context keys this step must have
method optional(--> List) { () }         # Context keys this step can use
method provides(--> List) { ... }        # Context keys this step writes
method execute(Context:D $ctx --> Nil) { ... }  # Do the work
```

LLM::Data::Pipeline::Context
----------------------------

String-keyed data bag flowing through the pipeline. Values must be JSON-serializable.

```raku
my LLM::Data::Pipeline::Context $ctx .= new;
$ctx.set('key', 'value');
$ctx.get('key');              # 'value'
$ctx.has('key');              # True
$ctx.keys;                   # Sorted list of all keys

# Serialization (for checkpointing)
my %snap = $ctx.snapshot;    # Deep-copy Hash via JSON round-trip
my $restored = LLM::Data::Pipeline::Context.from-snapshot(%snap);
```

LLM::Data::Pipeline::Plan
-------------------------

Ordered list of steps with dependency validation.

```raku
my LLM::Data::Pipeline::Plan $plan .= new;
$plan.add-step(StepA.new);
$plan.add-step(StepB.new);
$plan.steps;                          # List of steps in order
$plan.validate($ctx);                 # Dies with diagnostic if deps unsatisfied
```

Validation walks steps in order, tracking available keys from each step's `provides`. Reports all missing keys and duplicate step names in one error.

LLM::Data::Pipeline::Runner
---------------------------

Executes a Plan against a Context with checkpointing, surfacing everything observable as a typed [event stream](#LLM::Data::Pipeline::Event).

```raku
my LLM::Data::Pipeline::Runner $runner .= new(
    on-event => -> LLM::Data::Pipeline::Event $e {
        given $e {
            when LLM::Data::Pipeline::Event::StepStarted {
                say "→ {$e.step} ({$e.ordinal}/{$e.total})";
            }
            when LLM::Data::Pipeline::Event::StepCompleted {
                say "✓ {$e.step} in {$e.duration.round(0.01)}s";
            }
            when LLM::Data::Pipeline::Event::RunCompleted {
                say "done: {$e.steps-run} run, {$e.steps-skipped} skipped";
            }
        }
    }
);

# Run from beginning (validates first)
my $ctx = $runner.run($plan, $ctx, :checkpoint-path($path));

# Resume from checkpoint (completed steps arrive as step-skipped events)
my $ctx = $runner.resume($plan, $checkpoint-path);
```

Checkpoints are JSON files written after each step, containing completed step names and the full context snapshot. Writes use temp file + rename for atomicity.

### Observing a run

`&.on-event` is the primary hook: a synchronous callback invoked once per event on the run thread, in strict `seq` order. Handler exceptions are shielded (logged via `note`) so a broken observer can neither abort the run nor cause later events to be dropped.

A secondary `method events(--` Supply)> mirrors the same stream for reactive consumers. Taps run **synchronously on the run thread**, so tap before calling `run`/`resume`; the Supply receives `done` when the run finishes, on normal completion **and** on failure (`run-failed` is emitted first, then the exception is rethrown, then `done` fires). Bridge with `.Channel` for thread isolation.

```raku
$runner.events.tap(
    -> LLM::Data::Pipeline::Event $e { say to-json($e.to-hash) },
    done => { say 'stream closed' },
);
$runner.run($plan, $ctx);   # tap BEFORE running
```

The legacy `&.on-step` string callback (`'skip'`/`'start'`/`'complete'`) is retained as a thin shim over the event stream, firing identically to prior releases. It is **deprecated** and will be removed at `1.0` — migrate to `&.on-event`.

### Ordering guarantees

  * Events form a **per-run total order** by `seq` (contiguous, starting at `1`, no gaps).

  * `step-started` precedes its item events, which precede that step's `step-completed`/`step-failed` (item events arrive from `0.4.0`).

  * Cross-item ordering is defined **only by `seq`**.

`run-id` is stable within a single `run`/`resume` call and differs between calls. A validation failure throws before `run-started` is emitted.

### Step retries

Each plain step runs under the `&.step-retry` [RetryPolicy](#LLM::Data::Pipeline::RetryPolicy). The default is a single attempt, which preserves the pre-0.3.0 contract exactly — a failing step emits `run-failed` and rethrows its **original** exception (so domain types like `X::LLM::Data::Inference::Exhausted` keep flowing out).

```raku
# Opt into retries for every plain step:
my $runner = LLM::Data::Pipeline::Runner.new(
    step-retry => LLM::Data::Pipeline::RetryPolicy.new(:max-attempts(3)),
);
```

With `max-attempts` above 1, each failed attempt emits `step-failed` (`will-retry` True, `retry-delay` = the backoff), the Runner waits (via the injectable `&.schedule-after`), then emits `step-retry`. When the budget is spent it emits a final `step-failed` (`will-retry` False), then `run-failed`, then throws `X::LLM::Data::Pipeline::StepExhausted` carrying the step name, attempt count, and the last error.

**Retry implies idempotency**: a failed attempt's Context mutations are not rolled back. A step that opts into retries must tolerate its own partial writes; one that cannot must keep `max-attempts` at 1.

`&.now` and `&.schedule-after` are injectable so tests can drive a virtual clock with zero real sleeps.

LLM::Data::Pipeline::RetryPolicy
--------------------------------

Exponential-backoff retry budget: `delay-for(attempt) = min(max-delay, base-delay * 2 ** (attempt - 1)) + rand * jitter` (1-based `attempt`). `exhausted($attempts)` reports when the budget is spent. Defaults: `max-attempts` 3, `base-delay` 5s, `max-delay` 120s, `jitter` 0.5s — deliberately slower than the inference layer's per-call backoff, since it is the outer retry ring above `Task`'s own fast fallback chain.

LLM::Data::Pipeline::Exceptions
-------------------------------

`X::LLM::Data::Pipeline::StepExhausted` (`step`, `attempts`, `last-error`) is thrown when a step's retry budget is spent. `X::LLM::Data::Pipeline::Cancelled`, `ItemsDead`, and `CheckpointDrift` are declared now and thrown from `0.4.0`.

LLM::Data::Pipeline::Event
--------------------------

Every observable moment in a run is an immutable object composing the `LLM::Data::Pipeline::Event` role, which supplies three envelope fields — `UInt $.seq` (per-run, from 1), `Instant $.at`, `Str $.run-id` — plus `kind` (a kebab-case discriminator) and `to-hash` (a JSON-safe Hash; `at` serializes to a POSIX epoch-seconds number).

```raku
use LLM::Data::Pipeline::Event;
use JSON::Fast;

# Pattern-match on class, or switch on the kind string:
given $e {
    when LLM::Data::Pipeline::Event::RunStarted   { ... }
    when LLM::Data::Pipeline::Event::StepCompleted { ... }
}

say to-json($e.to-hash);   # JSON-safe log line, round-trips via from-json
```

The full taxonomy is declared now; the Stage-2 subset is emitted by this release. Reserved kinds let consumers pattern-match today and start receiving events in the noted release without a breaking change.

<table class="pod-table">
<caption>Pipeline event taxonomy</caption>
<thead><tr>
<th>Kind</th> <th>Class</th> <th>Payload fields</th> <th>Status</th>
</tr></thead>
<tbody>
<tr> <td>run-started</td> <td>RunStarted</td> <td>plan-size, resumed, checkpoint-path</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>step-skipped</td> <td>StepSkipped</td> <td>step, ordinal, total, description</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>step-started</td> <td>StepStarted</td> <td>step, ordinal, total, description</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>step-completed</td> <td>StepCompleted</td> <td>step, ordinal, total, description, duration, items-done?, items-dead?</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>checkpoint-written</td> <td>CheckpointWritten</td> <td>path, trigger</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>run-completed</td> <td>RunCompleted</td> <td>duration, steps-run, steps-skipped, items-done, items-dead</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>run-failed</td> <td>RunFailed</td> <td>step, error, exception</td> <td>emitted (0.2.0)</td> </tr> <tr> <td>step-failed</td> <td>StepFailed</td> <td>step, attempt, error, exception, will-retry, retry-delay</td> <td>emitted (0.3.0)</td> </tr> <tr> <td>step-retry</td> <td>StepRetry</td> <td>step, attempt, delay, error</td> <td>emitted (0.3.0)</td> </tr> <tr> <td>item-started</td> <td>ItemStarted</td> <td>step, key, attempt</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>item-completed</td> <td>ItemCompleted</td> <td>step, key, attempt, duration</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>item-failed</td> <td>ItemFailed</td> <td>step, key, attempt, error, exception</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>item-dead-lettered</td> <td>ItemDeadLettered</td> <td>step, key, attempts, error</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>item-requeued</td> <td>ItemRequeued</td> <td>step, key</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>progress</td> <td>Progress</td> <td>step, done, dead, total, in-flight, pending</td> <td>emitted (0.4.0; also at activation from 0.5.1)</td> </tr> <tr> <td>run-cancelled</td> <td>RunCancelled</td> <td>step, items-done, items-dead</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>telemetry</td> <td>Telemetry</td> <td>step, key, stage, data</td> <td>emitted (0.4.0)</td> </tr> <tr> <td>run-retry</td> <td>RunRetry</td> <td>attempt, max-attempts, delay, error, exception</td> <td>emitted (0.5.0)</td> </tr>
</tbody>
</table>

An item step emits one `progress` at **activation** (from `0.5.1`), before its first `item-started`, so a progress bar can show `0/N` for a fresh stage and `done/N` for a resumed one immediately instead of staying blank until the stage's first item finishes.

LLM::Data::Pipeline::Step::Items
--------------------------------

A step whose body is a set of items processed in parallel by the Runner. Instead of `execute`, implement `items` (deterministic, JSON-safe), `process-item` (worker thread; no Context mutation; at-least-once), and `finalize` (coordinator thread; assembles `provides` from the reserved `"{name}/items"` key). Per-item retries dead-letter to a crash-safe JSONL journal beside the checkpoint; a run can be resumed, cancelled, or `resume(:retry-dead)`'d. See [LLM::Data::Pipeline::Runner](LLM::Data::Pipeline::Runner) for the coordinator model, checkpoint v2, DLQ schema, and failure-mode table.

```raku
use LLM::Data::Pipeline::Step::Items;

class TagChunks does LLM::Data::Pipeline::Step::Items {
    method name(--> Str:D) { 'tag-chunks' }
    method description(--> Str:D) { 'Tag every chunk' }
    method provides(--> List) { ('tag-chunks/items', 'tags') }
    method items($ctx --> Iterable) { $ctx.get('chunks').list }
    method item-key(Int:D $i, $chunk --> Str:D) { $chunk<id> }
    method process-item($ctx, $chunk, Str:D $key --> Any) { tag-one($chunk) }
    method finalize($ctx --> Nil) {
        $ctx.set('tags', $ctx.get('tag-chunks/items').values.flat.unique.sort.list);
    }
}

my $runner = LLM::Data::Pipeline::Runner.new(:degree(8));
$runner.run($plan, $ctx, :checkpoint-path('run.checkpoint.json'.IO));
# Dead items are journaled to run.dlq.jsonl; retry them once fixed:
$runner.resume($plan, 'run.checkpoint.json'.IO, :retry-dead);
```

run-until-done
--------------

Supervise a pipeline to completion across whole-run retries — the outer ring above per-item and per-step retries. Attempt 1 resumes an existing checkpoint (so it composes with an external re-invoker after a crash), else runs; transient failures back off (`run-retry` event) and resume, while validation / cancellation / drift rethrow immediately. See [LLM::Data::Pipeline::Runner](LLM::Data::Pipeline::Runner) for the full contract and the "Operational recipes" section (in-process retries vs external supervisor, DLQ triage, the retry-dead decision guide).

```raku
my $ctx = $runner.run-until-done(
    $plan, $seed-ctx,
    checkpoint-path => 'run.checkpoint.json'.IO,
    run-retry       => LLM::Data::Pipeline::RetryPolicy.new(:max-attempts(5), :base-delay(30)),
    retry-dead      => True,   # requeue transiently-dead items on each attempt
);
```

AUTHOR
======

Matt Doughty <matt@apogee.guru>

COPYRIGHT AND LICENSE
=====================

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

