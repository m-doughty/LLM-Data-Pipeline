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
    :on-step(-> $name, $event { say "$name: $event" })
);
$runner.run($plan, $ctx, :checkpoint-path('checkpoint.json'.IO));

say $ctx.get('output');  # "processed: hello"

# Resume from checkpoint
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

Executes a Plan against a Context with checkpointing.

```raku
my LLM::Data::Pipeline::Runner $runner .= new(
    :on-step(-> Str:D $name, Str:D $event { ... })  # 'start', 'complete', 'skip'
);

# Run from beginning (validates first)
my $ctx = $runner.run($plan, $ctx, :checkpoint-path($path));

# Resume from checkpoint (skips completed steps)
my $ctx = $runner.resume($plan, $checkpoint-path);
```

Checkpoints are JSON files written after each step, containing completed step names and the full context snapshot. Writes use temp file + rename for atomicity.

AUTHOR
======

Matt Doughty <matt@apogee.guru>

COPYRIGHT AND LICENSE
=====================

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

