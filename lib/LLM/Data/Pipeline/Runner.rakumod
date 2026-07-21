use JSON::Fast;
use Digest::SHA256::Native;
use JSONL::Writer;
use JSONL::Reader;
use LLM::Data::Pipeline::Plan;
use LLM::Data::Pipeline::Context;
use LLM::Data::Pipeline::Step;
use LLM::Data::Pipeline::Step::Items;
use LLM::Data::Pipeline::Event;
use LLM::Data::Pipeline::RetryPolicy;
use LLM::Data::Pipeline::Exceptions;

unit class LLM::Data::Pipeline::Runner;

#| Primary observability hook: a synchronous callback invoked on the run thread,
#| in strict C<seq> order, once per emitted event. Handler exceptions are
#| shielded (logged via C<note>) so a misbehaving observer can never break a run.
has &.on-event;

#| Deprecated string-callback shim, kept for backwards compatibility. Fires only
#| for 'skip'/'start'/'complete', driven off the corresponding step events.
#| Removed at 1.0 — migrate to C<&.on-event>.
has &.on-step;

#| Retry budget applied to every plain step. Defaults to a single attempt, which
#| preserves the pre-0.3.0 abort-on-first-failure contract (and rethrows the
#| step's original exception). Set C<max-attempts> above 1 to opt into retries.
has LLM::Data::Pipeline::RetryPolicy:D $.step-retry .= new(:max-attempts(1));

#| Injectable clock. Used for run/step durations; override in tests for
#| determinism. Event C<at> timestamps always use the real wall clock.
has &.now = { now };

#| Injectable timer. Returns a Promise kept after C<$s> seconds (then running
#| C<&cb>); the Runner C<await>s it to back off between step retries, and item
#| retries re-enter the coordinator inbox through it. Override in tests to make
#| backoff instantaneous while capturing the requested delays.
has &.schedule-after = -> Real $s, &cb { Promise.in($s).then(&cb) };

#| Default worker parallelism for C<Step::Items> steps (per-step overridable via
#| C<degree>). C<1> is the sequential mode — same code path, one worker.
has UInt:D $.degree = 4;

#| Default per-item retry budget for C<Step::Items> steps (per-step overridable
#| via C<item-retry>). Exhausted items are dead-lettered, not fatal by default.
has LLM::Data::Pipeline::RetryPolicy:D $.item-retry .= new(:max-attempts(3));

#| Optional cooperative-cancellation hook. Polled by the item coordinator once
#| before the initial dispatch and once after each inbox message; on a truthy
#| return the run stops dispatching, drains in-flight items, checkpoints, emits
#| C<run-cancelled>, and throws C<X::LLM::Data::Pipeline::Cancelled>.
has &.is-cancelled;

#| Item-completion checkpoint coalescing: write at most once per this many item
#| terminals. C<1> (default) writes after every item. Dead-letter, step-boundary,
#| and cancel writes are never coalesced away.
has UInt:D $.checkpoint-every = 1;

#| Minimum seconds between coalesced item checkpoints. C<0.0> (default) disables
#| time-based throttling.
has Real:D $.checkpoint-interval = 0.0;

# --- per-run mutable state (one run at a time per Runner instance) ---
has UInt        $!seq;
has Str         $!run-id;
has Supplier    $!supplier;
has Str         @!completed;
has             %!step-state;
has IO::Path    $!checkpoint-path;
has Channel     $!current-inbox;

#| A lazy L<Supply> mirror of the event stream. Every emitted event is also
#| C<.emit>ed here, and the Supply receives C<done> when the run finishes —
#| whether it returns normally or throws. NOTE: taps run B<synchronously> on the
#| run thread, so tap before calling C<run>/C<resume>; consumers wanting thread
#| isolation should bridge with C<.Channel>.
method events(--> Supply) {
	$!supplier //= Supplier.new;
	$!supplier.Supply;
}

method run(
	LLM::Data::Pipeline::Plan:D $plan,
	LLM::Data::Pipeline::Context:D $ctx,
	IO::Path :$checkpoint-path
	--> LLM::Data::Pipeline::Context:D
) {
	self!begin-run;
	LEAVE self!finish-run;
	$!checkpoint-path = $checkpoint-path;
	@!completed = ();
	%!step-state = %();
	$plan.validate($ctx);
	self!drive($plan.steps, $ctx, :!resumed);
	$ctx;
}

method resume(
	LLM::Data::Pipeline::Plan:D $plan,
	IO::Path:D $checkpoint-path,
	Bool :$retry-dead = False
	--> LLM::Data::Pipeline::Context:D
) {
	self!begin-run;
	LEAVE self!finish-run;
	die "Checkpoint file not found: $checkpoint-path" unless $checkpoint-path.e;
	$!checkpoint-path = $checkpoint-path;
	my LLM::Data::Pipeline::Context $ctx = self!load-checkpoint($checkpoint-path);
	$plan.validate($ctx);
	self!apply-retry-dead($plan.steps, $ctx) if $retry-dead;
	self!drive($plan.steps, $ctx, :resumed);
	$ctx;
}

#| Supervise a pipeline to completion across process-local retries: attempt 1 is
#| C<resume> if the checkpoint already exists (so this composes with an external
#| re-invoker after a process death), else C<run>; every subsequent attempt is
#| C<resume>. Transient failures back off via C<&.schedule-after> and retry;
#| unretryable ones rethrow immediately. Returns the completed Context.
method run-until-done(
	LLM::Data::Pipeline::Plan:D $plan,
	LLM::Data::Pipeline::Context:D $ctx,
	IO::Path:D :$checkpoint-path!,
	LLM::Data::Pipeline::RetryPolicy:D :$run-retry =
		LLM::Data::Pipeline::RetryPolicy.new(:max-attempts(5), :base-delay(30), :max-delay(600)),
	Bool :$retry-dead = False
	--> LLM::Data::Pipeline::Context:D
) {
	# Plan validation can never be fixed by retrying — check it up front (against
	# the seeded Context, exactly as run() would) so a malformed plan throws now.
	$plan.validate($ctx);

	my UInt $attempt = 1;
	loop {
		my LLM::Data::Pipeline::Context $result;
		my $failure;
		{
			$result = $checkpoint-path.e
				?? self.resume($plan, $checkpoint-path, :$retry-dead)
				!! self.run($plan, $ctx, :$checkpoint-path);
			CATCH {
				# Unretryable: cancellation is the caller's intent; drift can't be
				# resolved by retrying the same checkpoint.
				when X::LLM::Data::Pipeline::Cancelled     { .rethrow }
				when X::LLM::Data::Pipeline::CheckpointDrift { .rethrow }
				# Dead items are poison: without :retry-dead a rethrow would recur
				# identically, so treat ItemsDead as unretryable there. With
				# :retry-dead the next attempt requeues them, so it IS retryable.
				when X::LLM::Data::Pipeline::ItemsDead {
					.rethrow unless $retry-dead;
					$failure = $_;
				}
				default { $failure = $_; }
			}
		}
		return $result unless $failure.defined;

		# Retryable failure — rethrow unchanged once the budget is spent.
		$failure.rethrow if $run-retry.exhausted($attempt);

		my Real $delay = $run-retry.delay-for($attempt);
		self!emit-run-retry(
			attempt      => $attempt,
			max-attempts => $run-retry.max-attempts,
			delay        => $delay,
			error        => $failure.message,
			exception    => $failure.^name,
		);
		await &!schedule-after($delay, -> $ { });
		$attempt++;
	}
}

method !begin-run(--> Nil) {
	$!seq          = 0;
	$!run-id       = self!mint-run-id;
	@!completed    = ();
	%!step-state   = %();
	$!current-inbox = Nil;
}

#| Load a checkpoint into C<@!completed> / C<%!step-state>, returning the
#| rehydrated Context. Accepts both v2 (with C<version>/C<step-state>) and legacy
#| v1 (no C<version> key) checkpoints. A v2 checkpoint's C<run-id> is adopted so
#| the DLQ/checkpoint lineage stays stable across resumes.
method !load-checkpoint(IO::Path:D $path --> LLM::Data::Pipeline::Context:D) {
	my %cp = from-json($path.slurp);
	@!completed = %cp<completed-steps>.list.map(*.Str);
	%!step-state = (%cp<step-state> // %()).clone;
	$!run-id = %cp<run-id> if %cp<run-id>:exists && %cp<run-id>.defined;
	LLM::Data::Pipeline::Context.from-snapshot(%cp<context>);
}

method !finish-run(--> Nil) {
	with $!supplier {
		.done;
		$!supplier = Nil;
	}
}

method !mint-run-id(--> Str:D) {
	my Str $rand = (^16).map({ (^16).pick.base(16) }).join.lc;
	sprintf('%x-%s', now.to-posix[0].Int, $rand);
}

method !drive(
	@steps,
	LLM::Data::Pipeline::Context:D $ctx,
	Bool:D :$resumed
	--> Nil
) {
	my $t0 = &!now.();
	self!emit: LLM::Data::Pipeline::Event::RunStarted,
		plan-size       => @steps.elems,
		resumed         => $resumed,
		checkpoint-path => ($!checkpoint-path.defined ?? $!checkpoint-path.Str !! Str);

	my Set $completed-set = @!completed.Set;
	my UInt $total = @steps.elems;
	my UInt $ordinal = 0;
	my UInt $steps-run = 0;
	my UInt $steps-skipped = 0;
	my UInt $run-items-done = 0;
	my UInt $run-items-dead = 0;
	my Str  $failed-step;

	{
		CATCH {
			# A cooperative cancellation is not a failure: the item engine already
			# emitted run-cancelled. Rethrow without a second (run-failed) terminal.
			when X::LLM::Data::Pipeline::Cancelled { .rethrow }
			default {
				self!emit: LLM::Data::Pipeline::Event::RunFailed,
					step      => $failed-step,
					error     => .message,
					exception => .^name;
				.rethrow;
			}
		}

		for @steps -> LLM::Data::Pipeline::Step $step {
			$ordinal++;
			if $step.name (elem) $completed-set {
				$steps-skipped++;
				self!emit: LLM::Data::Pipeline::Event::StepSkipped,
					step        => $step.name,
					ordinal     => $ordinal,
					total       => $total,
					description => $step.description;
				next;
			}

			self!emit: LLM::Data::Pipeline::Event::StepStarted,
				step        => $step.name,
				ordinal     => $ordinal,
				total       => $total,
				description => $step.description;

			$failed-step = $step.name;
			my $s0 = &!now.();
			my Bool $is-items = $step ~~ LLM::Data::Pipeline::Step::Items;
			my %summary = $is-items
				?? self!run-item-step($step, $ctx)
				!! do { self!run-step($step, $ctx); %() };

			@!completed.push($step.name);
			$completed-set = @!completed.Set;

			if $!checkpoint-path.defined {
				self!write-checkpoint($ctx);
				self!emit: LLM::Data::Pipeline::Event::CheckpointWritten,
					path    => $!checkpoint-path.Str,
					trigger => 'step';
			}

			$steps-run++;
			if $is-items {
				$run-items-done += %summary<items-done>;
				$run-items-dead += %summary<items-dead>;
			}
			self!emit: LLM::Data::Pipeline::Event::StepCompleted,
				step        => $step.name,
				ordinal     => $ordinal,
				total       => $total,
				description => $step.description,
				duration    => (&!now.() - $s0).Num,
				items-done  => ($is-items ?? %summary<items-done> !! UInt),
				items-dead  => ($is-items ?? %summary<items-dead> !! UInt);

			if $is-items && $step.fail-on-dead && %summary<items-dead> > 0 {
				die X::LLM::Data::Pipeline::ItemsDead.new(
					step      => $step.name,
					dead-keys => %summary<dead-keys>.list,
				);
			}
		}
	}

	self!emit: LLM::Data::Pipeline::Event::RunCompleted,
		duration      => (&!now.() - $t0).Num,
		steps-run     => $steps-run,
		steps-skipped => $steps-skipped,
		items-done    => $run-items-done,
		items-dead    => $run-items-dead;
}

#| Execute one plain step under the configured retry policy.
#|
#| With C<max-attempts> at 1 (the default), this is a bare C<execute> whose
#| exception propagates unchanged — identical to the pre-0.3.0 contract. With a
#| larger budget it retries: each failed attempt emits C<step-failed>, and if
#| another attempt remains it backs off via C<&.schedule-after> (blocking the run
#| thread — correct here, there are no worker threads yet) and emits
#| C<step-retry>. When the budget is spent it emits a final C<step-failed> with
#| C<will-retry> False and throws C<X::LLM::Data::Pipeline::StepExhausted>
#| (carrying the last error), which the outer handler turns into C<run-failed>.
#|
#| NOTE: a failed attempt's Context mutations are NOT rolled back. Step retry
#| therefore assumes the step is idempotent or tolerant of its own partial
#| writes; steps that cannot promise this must keep C<max-attempts> at 1.
method !run-step(LLM::Data::Pipeline::Step:D $step, LLM::Data::Pipeline::Context:D $ctx --> Nil) {
	if $!step-retry.max-attempts <= 1 {
		$step.execute($ctx);
		return;
	}

	my UInt $attempt = 1;
	loop {
		my $error;
		my Bool $ok = False;
		{
			$step.execute($ctx);
			$ok = True;
			CATCH { default { $error = $_ } }
		}
		return if $ok;

		my Bool $will-retry = !$!step-retry.exhausted($attempt);
		my Real $delay = $will-retry ?? $!step-retry.delay-for($attempt) !! 0.0;

		self!emit: LLM::Data::Pipeline::Event::StepFailed,
			step        => $step.name,
			attempt     => $attempt,
			error       => $error.message,
			exception   => $error.^name,
			will-retry  => $will-retry,
			retry-delay => $delay;

		unless $will-retry {
			die X::LLM::Data::Pipeline::StepExhausted.new(
				step       => $step.name,
				attempts   => $attempt,
				last-error => $error,
			);
		}

		await &!schedule-after($delay, -> $ { });
		$attempt++;

		self!emit: LLM::Data::Pipeline::Event::StepRetry,
			step    => $step.name,
			attempt => $attempt,
			delay   => $delay,
			error   => $error.message;
	}
}

#| Assign envelope fields (seq/at/run-id), then dispatch. C<seq> is the per-run
#| sequence (increments from 1).
method !emit(LLM::Data::Pipeline::Event:U $class, *%payload --> Nil) {
	self!dispatch-event: $class.new(
		seq    => ++$!seq,
		at     => now,
		run-id => $!run-id,
		|%payload,
	);
}

#| Dispatch an already-built event synchronously to the on-step shim, the
#| &.on-event callback, and the events() Supplier — each in an independently
#| shielded step so one throwing observer can't skip the others.
method !dispatch-event(LLM::Data::Pipeline::Event:D $event --> Nil) {
	if &!on-step.defined {
		my Str $legacy = do given $event.kind {
			when 'step-skipped'   { 'skip' }
			when 'step-started'   { 'start' }
			when 'step-completed' { 'complete' }
			default               { Str }
		};
		if $legacy.defined {
			try &!on-step($event.step, $legacy);
			note "on-step handler failed: $!" if $!;
		}
	}

	if &!on-event.defined {
		try &!on-event($event);
		note "on-event handler failed: $!" if $!;
	}

	with $!supplier {
		.emit($event);
	}
}

#| Emit a C<run-retry> supervision event between two attempts of
#| C<run-until-done>. It deliberately sits OUTSIDE any single run's C<seq>
#| sequence — each inner run/resume owns its own 1..N stream and has already
#| ended (its Supplier is C<done>) — so it is stamped with C<seq> 0 and the
#| lineage C<run-id> (the checkpoint's, from attempt 2 onward) and delivered via
#| the persistent C<&.on-event> callback.
method !emit-run-retry(*%payload --> Nil) {
	return unless &!on-event.defined || $!supplier.defined;
	self!dispatch-event: LLM::Data::Pipeline::Event::RunRetry.new(
		seq    => 0,
		at     => now,
		run-id => ($!run-id // 'run-until-done'),
		|%payload,
	);
}

# =========================================================================
# Parallel item engine (Step::Items)
# =========================================================================

#| Drive one C<Step::Items> step through the single-coordinator engine and
#| return C<{ items-done, items-dead, dead-keys }>.
#|
#| The coordinator (this thread) owns ALL mutable state. C<degree> worker
#| C<start> blocks pull jobs from a work Channel, run C<process-item> under a
#| C<try>, and message results back through an inbox Channel — they never touch
#| state, files, or events. Retries and telemetry re-enter through the inbox, so
#| every state transition (and thus event C<seq>, checkpoint, and DLQ write)
#| happens in exactly one place: the inbox loop.
method !run-item-step(
	LLM::Data::Pipeline::Step::Items:D $step,
	LLM::Data::Pipeline::Context:D $ctx
	--> Hash
) {
	my Str $name = $step.name;

	# --- Activation: materialize items once, deterministically. ---
	my @items = $step.items($ctx).cache;
	my Int $count = @items.elems;

	my %item-by-key;
	my Str @keys;
	for @items.kv -> $i, $item {
		my Str:D $key = $step.item-key($i, $item);
		die "{$name}: duplicate item key '$key' — item-key must be unique across the item set"
			if %item-by-key{$key}:exists;
		%item-by-key{$key} = $item;
		@keys.push($key);
	}

	# Fingerprint over the JSON of the key list (unambiguous — no separator that a
	# key could contain) AND the JSON of the materialized items. items() is
	# deterministic-given-Context by contract, so hashing the items is free and
	# catches value-level drift that stable keys (e.g. index keys) cannot.
	my Str $keys-sha  = sha256-hex(to-json(@keys.List, :sorted-keys));
	my Str $items-sha = sha256-hex(to-json(@items.List, :sorted-keys));
	my %fingerprint = count => $count, keys-sha256 => $keys-sha, items-sha256 => $items-sha;

	# --- Drift guard: recompute vs the stored fingerprint on resume. ---
	my %prior = %!step-state{$name} // %();
	if %prior<fingerprint>:exists {
		my %stored = %prior<fingerprint>;
		if %stored<count> != $count
			|| %stored<keys-sha256> ne $keys-sha
			|| (%stored<items-sha256>.defined && %stored<items-sha256> ne $items-sha) {
			die X::LLM::Data::Pipeline::CheckpointDrift.new(
				step                 => $name,
				expected-fingerprint => %stored.Hash,
				got                  => %fingerprint,
			);
		}
	}

	# --- Coordinator state (single-writer). ---
	my %done;      # key => result value
	my %attempts;  # key => attempts made so far
	my %dead;      # key => True
	my %history;   # key => [ { attempt, at, error, duration } ]
	my %first-at;  # key => posix seconds of first start
	my %telem;     # key => { attempts, summary } (last 'exhausted' telemetry)
	my %in-flight; # key => True (dispatched, awaiting a result)

	# Rehydrate prior progress (resume into an in-progress step).
	for (%prior<results> // %()).kv -> $k, $v { %done{$k} = $v; }
	for (%prior<attempts> // %()).kv -> $k, $v { %attempts{$k} = $v; }
	for (%prior<dead> // %()).keys -> $k { %dead{$k} = True; }

	%!step-state{$name} //= %();
	%!step-state{$name}<fingerprint> = %fingerprint;
	%!step-state{$name}<started-at> //= DateTime.now(:timezone(0)).Str;

	my UInt $degree = $step.degree > 0 ?? $step.degree !! $!degree;
	$degree = 1 if $degree < 1;
	my LLM::Data::Pipeline::RetryPolicy $policy =
		$step.item-retry.defined ?? $step.item-retry !! $!item-retry;

	my Bool $cancelling = False;

	# --- Freeze the Context and spin up workers. ---
	$ctx.freeze;
	# The Context cannot change while frozen, so snapshot it exactly once (here,
	# single-threaded, before any worker starts). Every per-item / dead-letter /
	# cancel checkpoint reuses this snapshot — no repeated whole-Context
	# serialization, and no Context iteration concurrent with worker reads.
	my %ctx-snapshot = $ctx.snapshot;
	my $work  = Channel.new;
	my $inbox = Channel.new;
	$!current-inbox = $inbox;

	my @workers = (^$degree).map: {
		start {
			for $work.list -> %job {
				my Str $key = %job<key>;
				my Int $attempt = %job<attempt>;
				# The whole per-job body is wrapped so it can NEVER leave without
				# sending a result for $key. If it did, outstanding() would never
				# decrement and the coordinator would hang forever on receive.
				{
					$inbox.send: { kind => 'started', :$key, :$attempt };
					my $t0 = now;
					my $value;
					my Bool $ok = False;
					my $err;
					{
						$value = $step.process-item($ctx, %item-by-key{$key}, $key);
						$ok = True;
						CATCH { default { $err = $_ } }
					}
					my Num $duration = (now - $t0).Num;
					if $ok {
						$inbox.send: { kind => 'result', :$key, :$attempt, :ok, :$value, :$duration };
					} else {
						# Compute error text defensively: a hostile exception's own
						# .message/.^name accessor may throw.
						my Str $emsg = (try { $err.message }) // (try { $err.gist }) // 'unknown error';
						my Str $etype = (try { $err.^name }) // 'unknown';
						$inbox.send: {
							kind => 'result', :$key, :$attempt, ok => False,
							error => $emsg, exception => $etype, :$duration,
						};
					}
					# Last resort: anything above that still managed to throw (e.g.
					# the started send, or hash construction) must not strand $key.
					CATCH {
						default {
							$inbox.send: {
								kind => 'result', :$key, :$attempt, ok => False,
								error => ((try { .message }) // 'worker error'),
								exception => ((try { .^name }) // 'unknown'),
								duration => 0e0,
							};
						}
					}
				}
			}
		}
	}

	# Idempotent teardown: close the work Channel, join the workers, clear the
	# active inbox, and (critically) THAW the caller's Context. Runs on the normal
	# path (explicitly, so workers are joined before the reserved-key writes) AND
	# via LEAVE on any exception between here and completion — so a fatal DLQ
	# append, a checkpoint write failure, a non-serializable result, or a throwing
	# is-cancelled can never leak workers or leave the Context permanently frozen.
	my Bool $joined = False;
	my sub teardown() {
		return if $joined;
		$joined = True;
		try $work.close;
		try await Promise.allof(@workers);
		$!current-inbox = Nil;
		$ctx.thaw;
	}
	LEAVE teardown();

	# --- Coordinator helpers (all run on this thread). ---
	my Int $since-cp = 0;
	my $last-cp-at = now;

	my sub sync-state(Bool :$prune-results = False) {
		%!step-state{$name}<done>     = %done.keys.map({ $_ => True }).Hash;
		%!step-state{$name}<results>  = $prune-results ?? %() !! %done.Hash;
		%!step-state{$name}<attempts> = %attempts.Hash;
		%!step-state{$name}<dead>     = %dead.keys.map({ $_ => True }).Hash;
	}

	my sub write-cp(Str:D :$trigger!, Bool :$force = False) {
		return without $!checkpoint-path;
		$since-cp++;
		my Bool $due-count = $since-cp >= $!checkpoint-every;
		my Bool $due-time =
			$!checkpoint-interval <= 0 || (now - $last-cp-at).Real >= $!checkpoint-interval;
		return unless $force || ($due-count && $due-time);
		sync-state;
		self!write-checkpoint($ctx, :snapshot(%ctx-snapshot));
		$since-cp = 0;
		$last-cp-at = now;
		self!emit: LLM::Data::Pipeline::Event::CheckpointWritten,
			path => $!checkpoint-path.Str, trigger => $trigger;
	}

	my sub emit-progress() {
		self!emit: LLM::Data::Pipeline::Event::Progress,
			step      => $name,
			total     => $count,
			done      => %done.elems,
			dead      => %dead.elems,
			in-flight => %in-flight.elems,
			pending   => ($count - %done.elems - %dead.elems - %in-flight.elems);
	}

	# Keys ready to (re)dispatch but not yet in flight. Dispatch is throttled to
	# at most $degree in flight, so cancellation can actually stop pending work
	# (only the ≤ $degree in-flight items must drain) and the work Channel never
	# buffers more than $degree jobs.
	my Str @ready = @keys.grep({ !(%done{$_}:exists) && !(%dead{$_}:exists) }).list;

	my sub dispatch(Str:D $key) {
		my Int $attempt = (%attempts{$key} // 0) + 1;
		%in-flight{$key} = True;
		$work.send: { :$key, :$attempt };
	}

	my sub pump() {
		return if $cancelling;
		while %in-flight.elems < $degree && @ready {
			dispatch(@ready.shift);
		}
	}

	my sub outstanding() { $count - %done.elems - %dead.elems }

	# --- Initial fill (respecting the degree throttle). ---
	$cancelling = True if &!is-cancelled.defined && &!is-cancelled.();
	pump();

	# --- The one inbox loop: every transition happens here. ---
	while ($cancelling ?? %in-flight.elems > 0 !! outstanding() > 0) {
		my %msg = $inbox.receive;
		# Poll cancellation BEFORE acting on the message, so a 'wake-retry' that
		# arrives after cancel was requested (e.g. the only outstanding work was a
		# backoff timer) sees $cancelling and pump() no-ops — the item is NOT
		# redispatched and stays pending for a clean resume.
		$cancelling = True
			if !$cancelling && &!is-cancelled.defined && &!is-cancelled.();
		given %msg<kind> {
			when 'started' {
				%first-at{%msg<key>} //= now.to-posix[0].Num;
				self!emit: LLM::Data::Pipeline::Event::ItemStarted,
					step => $name, key => %msg<key>, attempt => %msg<attempt>;
			}
			when 'telemetry' {
				# Defence in depth: never let a message minted for another step
				# pollute this step's telemetry / DLQ inference block.
				if (%msg<step> // $name) ne $name {
					note "telemetry for step '{%msg<step>}' arrived while step "
						~ "'$name' is active; dropping.";
				}
				else {
					my %data = %msg<data>;
					if (%data<stage> // '') eq 'exhausted' {
						%telem{%msg<key>} = {
							attempts => (%data<attempts> // 0),
							summary  => (%data<summary> // ''),
						};
					}
					self!emit: LLM::Data::Pipeline::Event::Telemetry,
						step  => $name,
						key   => (%msg<key> // Str),
						stage => (%data<stage> // 'unknown'),
						data  => %data;
				}
			}
			when 'result' {
				my Str $key = %msg<key>;
				my Int $attempt = %msg<attempt>;
				%in-flight{$key}:delete;
				%attempts{$key} = $attempt;

				if %msg<ok> {
					%done{$key} = %msg<value>;
					self!emit: LLM::Data::Pipeline::Event::ItemCompleted,
						step => $name, key => $key, attempt => $attempt,
						duration => %msg<duration>;
					write-cp(trigger => 'item');
					emit-progress();
				} else {
					%history{$key}.push: {
						attempt  => $attempt,
						at       => now.to-posix[0].Num,
						error    => %msg<error>,
						duration => %msg<duration>,
					};
					self!emit: LLM::Data::Pipeline::Event::ItemFailed,
						step => $name, key => $key, attempt => $attempt,
						error => %msg<error>, exception => %msg<exception>;

					if $cancelling {
						# Abandoned by cancellation: neither retried nor dead-lettered;
						# stays pending for a clean resume.
					} elsif !$policy.exhausted($attempt) {
						my Real $delay = $policy.delay-for($attempt);
						&!schedule-after($delay, -> $ {
							$inbox.send: { kind => 'wake-retry', :$key };
						});
					} else {
						# Exhausted → dead-letter. DLQ FIRST, then checkpoint.
						%dead{$key} = True;
						self!write-dlq-record(
							kind      => 'dead',
							step      => $name,
							key       => $key,
							item      => %item-by-key{$key},
							attempts  => $attempt,
							history   => (%history{$key} // []),
							error     => { exception => %msg<exception>, message => %msg<error> },
							inference => (%telem{$key} // %()),
							first-at  => %first-at{$key},
						);
						self!emit: LLM::Data::Pipeline::Event::ItemDeadLettered,
							step => $name, key => $key, attempts => $attempt,
							error => %msg<error>;
						write-cp(trigger => 'dead-letter', :force);
						emit-progress();
					}
				}
				# A result (any outcome) freed an in-flight slot — refill it.
				pump();
			}
			when 'wake-retry' {
				@ready.push(%msg<key>);
				pump();
			}
		}
	}

	# --- Join workers and thaw the Context (before any reserved-key writes). ---
	teardown();

	if $cancelling {
		sync-state;
		if $!checkpoint-path.defined {
			self!write-checkpoint($ctx, :snapshot(%ctx-snapshot));
			self!emit: LLM::Data::Pipeline::Event::CheckpointWritten,
				path => $!checkpoint-path.Str, trigger => 'cancel';
		}
		self!emit: LLM::Data::Pipeline::Event::RunCancelled,
			step => $name, items-done => %done.elems, items-dead => %dead.elems;
		die X::LLM::Data::Pipeline::Cancelled.new(step => $name);
	}

	# --- Normal completion: reserved keys, finalize, prune in-progress results. ---
	$ctx.set("{$name}/items", %done.Hash);
	$ctx.set("{$name}/dead", %dead.keys.sort.list);
	$step.finalize($ctx);
	sync-state(:prune-results);

	%(
		items-done => %done.elems,
		items-dead => %dead.elems,
		dead-keys  => %dead.keys.sort.list,
	);
}

#| A thread-safe telemetry sink bound to this Runner. The returned closure, when
#| called with a Hash, forwards it to the inbox of the item step that was active
#| B<when the sink was minted> (a Channel send — safe from any thread). Binding
#| the inbox at mint time is deliberate: a late/async callback minted during step
#| A can then never land in step B's inbox and pollute B's telemetry or DLQ
#| inference block (it sends to A's now-drained inbox, which nobody reads). A
#| telemetry Hash with C<< stage => 'exhausted' >> (carrying C<attempts> and
#| C<summary>) becomes the DLQ record's C<inference> block for that (step, key).
#| If no item step is active when the sink is minted, it drops with a C<note>.
method telemetry-sink(Str:D :$step!, Str :$key --> Callable) {
	my $bound-inbox = $!current-inbox;   # capture at mint time, not call time
	-> %data {
		with $bound-inbox {
			.send: { kind => 'telemetry', :$step, key => ($key // Str), data => %data };
		} else {
			note "telemetry-sink($step): no active item step; dropping telemetry.";
		}
	}
}

#| Write the checkpoint as v2 (always), atomically (temp file + rename). Reflects
#| the current C<@!completed> and C<%!step-state>. C<:snapshot> supplies a
#| pre-computed Context snapshot (used during item processing, where the frozen
#| Context is snapshotted once); otherwise the Context is snapshotted here.
method !write-checkpoint(LLM::Data::Pipeline::Context:D $ctx, :$snapshot --> Nil) {
	return without $!checkpoint-path;
	my %checkpoint = %(
		version         => 2,
		run-id          => $!run-id,
		completed-steps => @!completed.list,
		context         => ($snapshot.defined ?? $snapshot !! $ctx.snapshot),
		step-state      => %!step-state,
		updated-at      => DateTime.now(:timezone(0)).Str,
	);
	my IO::Path $tmp = $!checkpoint-path.sibling($!checkpoint-path.basename ~ '.tmp');
	$tmp.spurt(to-json(%checkpoint, :sorted-keys));
	$tmp.rename($!checkpoint-path);
}

#| Derive the DLQ path from the checkpoint path: C<foo.checkpoint.json> →
#| C<foo.dlq.jsonl>; otherwise the checkpoint's basename minus its final
#| extension, plus C<.dlq.jsonl>, as a sibling. C<Nil> when there is no
#| checkpoint path (no checkpoint ⇒ no DLQ file).
method !dlq-path(--> IO::Path) {
	return IO::Path without $!checkpoint-path;
	my Str $s = $!checkpoint-path.Str;
	if $s.ends-with('.checkpoint.json') {
		return $s.subst(/'.checkpoint.json' $/, '.dlq.jsonl').IO;
	}
	my Str $base = $!checkpoint-path.extension('').basename;
	$!checkpoint-path.sibling($base ~ '.dlq.jsonl');
}

#| Append one record to the DLQ journal (open/append/close — crash-safe). A DLQ
#| append failure is FATAL: it is deliberately not shielded, because the journal
#| is the one artifact this feature exists to produce.
method !write-dlq-record(
	Str:D :$kind!, Str:D :$step!, Str:D :$key!, :$item, Int:D :$attempts,
	:@history, :%error, :%inference, :$first-at
	--> Nil
) {
	my IO::Path $path = self!dlq-path;
	return without $path;
	my %record = (
		schema           => 1,
		kind             => $kind,
		run-id           => $!run-id,
		step             => $step,
		key              => $key,
		item-digest      => sha256-hex(to-json($item, :sorted-keys)),
		attempts         => $attempts,
		attempt-history  => @history,
		error            => %error,
		first-attempt-at => $first-at,
		dead-at          => now.to-posix[0].Num,
	);
	%record<inference> = %inference if %inference.elems;
	JSONL::Writer.new(:$path).append(%record);
}

# =========================================================================
# retry-dead (resume flag)
# =========================================================================

#| Requeue every dead item across all steps: clear it from C<dead>/C<attempts>,
#| verify its digest against the DLQ journal (C<CheckpointDrift> on mismatch),
#| append a C<requeued> journal record, and emit C<item-requeued>. Reopening a
#| step that was already completed removes it AND every later step from
#| C<@!completed> (they consumed its provides) and clears their item state, while
#| rehydrating the reopened step's successes from its reserved Context key so
#| only the formerly-dead items re-run.
method !apply-retry-dead(@steps, LLM::Data::Pipeline::Context:D $ctx --> Nil) {
	# Track the last DEAD record per (step, key) — NOT the last record overall.
	# A crash between appending a 'requeued' record and updating the checkpoint
	# would otherwise leave last-kind='requeued', silently bypassing digest
	# verification on the next retry-dead. The dead record holds the authoritative
	# digest, so we always verify against it.
	my Bool $dlq-present = False;
	my %dlq-dead;   # "step\0key" => last 'dead' record Hash
	my IO::Path $dlq = self!dlq-path;
	if $dlq.defined && $dlq.e {
		$dlq-present = True;
		for JSONL::Reader.new(:path($dlq)).list -> $line {
			my %r = $line.value;
			next unless %r<step>.defined && %r<key>.defined;
			%dlq-dead{"%r<step>\x[0]%r<key>"} = %r if (%r<kind> // '') eq 'dead';
		}
	}

	my %step-by-name = @steps.map({ .name => $_ });
	my Str @reopened;

	for %!step-state.keys.sort -> $name {
		my %st = %!step-state{$name};
		my @dead-keys = (%st<dead> // %()).keys.sort;
		next unless @dead-keys;
		my $step = %step-by-name{$name};
		next without $step;

		# Recompute items for digest verification.
		my @items = $step.items($ctx).cache;
		my %item-by-key;
		for @items.kv -> $i, $item {
			%item-by-key{$step.item-key($i, $item)} = $item;
		}

		for @dead-keys -> $key {
			my %journal = %dlq-dead{"$name\x[0]$key"} // %();
			if %journal {
				# Verify the item hasn't changed since it was dead-lettered.
				my Str $recomputed = sha256-hex(to-json(%item-by-key{$key}, :sorted-keys));
				if $recomputed ne %journal<item-digest> {
					die X::LLM::Data::Pipeline::CheckpointDrift.new(
						step                 => $name,
						expected-fingerprint => %journal<item-digest>,
						got                  => $recomputed,
					);
				}
			}
			elsif $dlq-present {
				# File exists but has no dead record for this key: proceed (the
				# checkpoint is authoritative for control flow; the journal is
				# forensic), but say so.
				note "retry-dead: no DLQ 'dead' record for ($name, $key); "
					~ "requeuing without a digest check.";
			}
			# No DLQ file at all ⇒ proceed silently (checkpoint is authoritative).
			(%!step-state{$name}<dead>){$key}:delete;
			(%!step-state{$name}<attempts>){$key}:delete;
			self!write-dlq-record(
				kind     => 'requeued',
				step     => $name,
				key      => $key,
				item     => %item-by-key{$key},
				attempts => 0,
			);
			self!emit: LLM::Data::Pipeline::Event::ItemRequeued, step => $name, key => $key;
		}
		@reopened.push($name);
	}

	self!invalidate-downstream(@steps, @reopened, $ctx);
}

#| Given the steps reopened by C<:retry-dead>, drop the earliest reopened
#| B<completed> step and every step after it from C<@!completed>; clear the later
#| steps' item state entirely, and rehydrate the reopened step's successes from
#| its reserved Context key so it re-runs only the requeued items.
method !invalidate-downstream(@steps, @reopened, LLM::Data::Pipeline::Context:D $ctx --> Nil) {
	my Str @names = @steps.map(*.name);
	my Set $completed-set = @!completed.Set;
	my $earliest = Inf;
	for @reopened -> $name {
		next unless $name (elem) $completed-set;
		my $idx = @names.first($name, :k);
		$earliest = $idx if $idx.defined && $idx < $earliest;
	}
	return if $earliest == Inf;

	my Set $keep = @names[^$earliest].Set;
	@!completed = @!completed.grep({ $_ (elem) $keep }).list;

	for @names[$earliest .. *] -> $name {
		if $name eq @names[$earliest] {
			my %items = $ctx.get("{$name}/items") // %();
			%!step-state{$name}<results> = %items.Hash;
			%!step-state{$name}<done>    = %items.keys.map({ $_ => True }).Hash;
		} else {
			%!step-state{$name}:delete;
		}
	}
}

=begin pod

=head1 NAME

LLM::Data::Pipeline::Runner - Executes a Plan against a Context, emitting a typed event stream

=head1 SYNOPSIS

=begin code :lang<raku>

use LLM::Data::Pipeline::Runner;
use LLM::Data::Pipeline::Event;

my LLM::Data::Pipeline::Runner $runner .= new(
	on-event => -> LLM::Data::Pipeline::Event $e {
		given $e {
			when LLM::Data::Pipeline::Event::RunStarted {
				say "run {$e.run-id} started ({$e.plan-size} steps, resumed={$e.resumed})";
			}
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

my $ctx = $runner.run($plan, $ctx, :checkpoint-path('checkpoint.json'.IO));

# Resume: completed steps arrive as step-skipped, the rest execute.
my $ctx2 = $runner.resume($plan, 'checkpoint.json'.IO);

=end code

=head1 DESCRIPTION

The Runner drives a L<Plan|LLM::Data::Pipeline::Plan> over a
L<Context|LLM::Data::Pipeline::Context>, validating dependencies first, then
executing each step in order with a JSON checkpoint written after every
successful step (temp-file + rename for atomicity). Everything observable is
surfaced as a typed L<LLM::Data::Pipeline::Event>.

=head2 Observing a run

=head3 C<&.on-event> (primary)

A synchronous callback, invoked once per event on the run thread in strict
C<seq> order:

=begin code :lang<raku>

my @log;
my $runner = LLM::Data::Pipeline::Runner.new(
	on-event => -> $e { @log.push($e.to-hash) },
);
$runner.run($plan, $ctx);

=end code

Because delivery is synchronous, ordering and backpressure are free: the next
step never starts until your handler returns. Handler exceptions are shielded
(reported via C<note>) so a broken observer can neither abort the run nor cause
later events to be dropped.

=head3 C<method events(--> Supply)> (secondary)

A L<Supply> mirror for reactive consumers:

=begin code :lang<raku>

my $runner = LLM::Data::Pipeline::Runner.new;
$runner.events.tap(
	-> $e { say $e.kind },
	done => { say 'stream closed' },
);
$runner.run($plan, $ctx);   # tap BEFORE running

=end code

The Supply is backed by a C<Supplier>; taps run B<synchronously on the run
thread> (the same caveat as C<&.on-event>). Tap before calling
C<run>/C<resume>, or you will miss events already emitted. The Supply receives
C<done> when the run finishes — on normal completion B<and> on failure (the
C<run-failed> event is emitted first, then the exception is rethrown, then
C<done> fires). A consumer wanting isolation from the run thread should bridge
with C<.Channel>. Each run closes its Supplier; call C<events> again for a fresh
Supply before the next run.

=head3 C<&.on-step> (deprecated)

The original string-callback API is retained as a thin shim over the event
stream and fires identically to previous releases — C<'skip'>, C<'start'>,
C<'complete'> — driven off C<step-skipped>/C<step-started>/C<step-completed>:

=begin code :lang<raku>

my $runner = LLM::Data::Pipeline::Runner.new(
	on-step => -> Str:D $name, Str:D $event { say "$name: $event" },
);

=end code

B<Deprecated> — it exposes only step transitions and no run/checkpoint/item
detail. It will be removed at C<1.0>; migrate to C<&.on-event>.

=head2 Ordering guarantees

=item Events form a B<per-run total order> by C<seq> (contiguous, starting at
      C<1>, no gaps).
=item C<step-started> precedes its item events, which precede that step's
      C<step-completed>/C<step-failed> (item events arrive from C<0.4.0>).
=item Cross-item ordering is defined B<only by C<seq>> — do not infer any other
      relationship between events of different items.

C<run-id> is stable for the lifetime of a single C<run>/C<resume> call and
differs between calls (minted per invocation). A validation failure throws
before C<run-started> is emitted, so a stream that opens with C<run-started>
means validation has already passed.

=head2 Step retries

Each plain step is executed under the C<&.step-retry>
L<RetryPolicy|LLM::Data::Pipeline::RetryPolicy>. The default is a single
attempt:

=begin code :lang<raku>

# Default: try each step once; on failure the run aborts and the step's
# ORIGINAL exception propagates unchanged (pre-0.3.0 contract).
my $runner = LLM::Data::Pipeline::Runner.new;

# Opt into retries for every plain step:
my $resilient = LLM::Data::Pipeline::Runner.new(
	step-retry => LLM::Data::Pipeline::RetryPolicy.new(
		:max-attempts(3), :base-delay(5), :max-delay(120),
	),
);

=end code

With C<max-attempts> at 1, a failing step behaves exactly as before: no
C<step-failed> event is emitted, C<run-failed> fires, and the B<original>
exception is rethrown (so domain types such as
C<X::LLM::Data::Inference::Exhausted> still flow out, and existing C<CATCH>
handlers keep matching).

With C<max-attempts> above 1, each failed attempt emits C<step-failed>
(C<will-retry> True, C<retry-delay> = the backoff); the Runner then waits that
long (via C<&.schedule-after>, blocking the run thread) and emits C<step-retry>
before the next attempt. When the budget is spent it emits a final
C<step-failed> (C<will-retry> False), then C<run-failed>, then throws
C<X::LLM::Data::Pipeline::StepExhausted> — a Pipeline-typed wrapper carrying the
step name, attempt count, and the C<last-error> (the final attempt's real
exception). Exhausted plain steps always abort the run; skipping them would be
unsound against C<provides>/C<requires>.

B<Retry implies idempotency.> A failed attempt's Context mutations are B<not>
rolled back — the same Context, with whatever partial writes the failed attempt
made, is handed to the next attempt. A step that opts into retries must be
idempotent or tolerant of its own partial writes; a step that cannot promise
this must keep C<max-attempts> at 1.

=head2 Injectable time

C<&.now> (default C<{ now }>) supplies the clock used for run and step
durations, and C<&.schedule-after> (default
C<< -> Real $s, &cb { Promise.in($s).then(&cb) } >>) supplies the retry timer.
Both are injectable so tests can run a virtual clock — capturing the requested
backoff delays and returning an already-kept Promise — with zero real sleeps.
Event C<at> timestamps always use the real wall clock.

=head1 PARALLEL ITEM STEPS (Step::Items)

A L<Step::Items|LLM::Data::Pipeline::Step::Items> is processed by the Runner's
item engine rather than a single C<execute>.

=head2 The single-coordinator model

The C<run> thread is the B<coordinator> and owns ALL mutable state (results,
attempts, dead set, in-flight set, checkpoint, DLQ). It spins up C<degree> worker
C<start> blocks that pull jobs from a work C<Channel>, run C<process-item> under
a C<try>, and message results back through a single inbox C<Channel>. Workers
B<never> touch state, files, or events. Retries and telemetry re-enter through
the inbox, so B<every> state transition happens in one place — the inbox loop.
There are no locks: event C<seq> ordering and single-writer checkpoint/DLQ fall
out for free. C<:degree(1)> is the sequential mode on the same code path.

Dispatch is throttled to at most C<degree> items in flight; the rest wait in a
ready queue. This bounds concurrency and lets cancellation actually stop pending
work (only the ≤ C<degree> in-flight items must drain).

The Context is B<frozen> for the duration of item processing: C<process-item>
runs on a worker thread and any C<set> throws. Reads are safe (nothing mutates
the data). The engine thaws before C<finalize>, then writes the reserved keys
C<"{name}/items"> (key→result Hash) and C<"{name}/dead"> (sorted List).

=head2 Knobs

=begin table :caption<Item-engine knobs>
Knob                 | Default | Meaning
==========================================================================================
degree               | 4       | Worker parallelism (per-step override via C<degree>)
item-retry           | 3 tries | Per-item RetryPolicy (per-step override via C<item-retry>)
is-cancelled         | (none)  | Cooperative cancel hook, polled initially + after each message
checkpoint-every     | 1       | Coalesce item checkpoints to 1 per N item terminals
checkpoint-interval  | 0.0     | Minimum seconds between coalesced item checkpoints
=end table

Dead-letter, step-boundary, and cancel checkpoints are B<never> coalesced away.

=head2 Checkpoint v2

Written atomically (temp file + rename) as:

=begin code :lang<json>

{ "version": 2, "run-id": "…", "completed-steps": [...], "context": {...},
  "step-state": { "<step>": {
      "fingerprint": {"count": N, "keys-sha256": "…", "items-sha256": "…"},
      "done": {"<key>": true}, "results": {"<key>": ...},
      "attempts": {"<key>": 2}, "dead": {"<key>": true}, "started-at": "…" } },
  "updated-at": "…" }

=end code

In-progress item results live in C<step-state.results> while the Context is
frozen (this is what makes mid-step checkpoints useful for resume); they are
promoted to the reserved Context key at C<finalize> and pruned. C<done> /
C<attempts> / C<dead> are kept after completion (C<:retry-dead> needs C<dead>).
Legacy v1 checkpoints (no C<version> key) are still accepted on resume.

At activation the engine materializes C<items()> once and records a
fingerprint: C<count>, C<keys-sha256> = SHA-256 of C<to-json> of the key list,
and C<items-sha256> = SHA-256 of C<to-json> of the materialized items. Resuming
into an in-progress step recomputes all three and throws
C<X::LLM::Data::Pipeline::CheckpointDrift> on any mismatch — so C<items-sha256>
catches value-level drift that stable keys (e.g. index keys) cannot. Duplicate
keys from C<item-key> abort at activation.

=head2 Dead-letter queue (DLQ)

C<foo.checkpoint.json> ⇒ C<foo.dlq.jsonl>, appended one record at a time
(open/append/close — crash-safe). No checkpoint path ⇒ no DLQ file (dead items
are still reported in-memory). Record shape:

=begin code :lang<json>

{ "schema": 1, "kind": "dead"|"requeued", "run-id": "…", "step": "…", "key": "…",
  "item-digest": "sha256…", "attempts": N,
  "attempt-history": [{"attempt":1,"at":…,"error":"…","duration":…}],
  "error": {"exception":"…","message":"…"},
  "inference": {"attempts":…,"summary":"…"},   // optional, from telemetry-sink
  "first-attempt-at": …, "dead-at": … }

=end code

The C<inference> block is the last telemetry Hash with C<< stage => 'exhausted' >>
for that (step, key); wire it via C<method telemetry-sink(:$step!, :$key)>, whose
returned closure only does a Channel send (thread-safe). Pipeline never imports
any inference types — the coupling is this one documented Hash shape.

Note the two attempt fields differ in scope: C<attempts> is the B<cumulative>
attempt count carried in the checkpoint across resumes, whereas
C<attempt-history> lists only the attempts made B<by the current process lineage
since the last resume>. Per-attempt history is deliberately not persisted in
step-state (it would bloat the checkpoint); after a resume the history restarts
while C<attempts> keeps counting.

=head2 Consistency rules

=item The B<checkpoint is authoritative> for control flow; the DLQ is a forensic,
      append-only journal.
=item Write order on exhaustion is B<DLQ first, then checkpoint>. A crash between
      the two yields a duplicate dead record on the next run; consumers B<dedupe
      by the last record per (run-id, step, key)>. This dedupe is sound only
      because C<run-id> is a B<stable lineage identifier>: C<run> mints it once
      and every C<resume> adopts the checkpoint's C<run-id>, so a C<dead> record
      and a later C<requeued> record for the same item share a key and collapse
      correctly. (A per-invocation run-id would split them and break dedupe.)
=item A DLQ append failure is B<fatal to the run> — the journal is the one
      artifact this feature exists to produce and is never silently dropped.
=item C<process-item> is B<at-least-once>; recording is exactly-once per
      checkpoint lineage. A pure C<process-item> is effectively exactly-once; any
      external side effect must be idempotent.

=head2 resume(:retry-dead)

Requeues every dead item (clearing it from C<dead>/C<attempts>, appending a
C<requeued> journal record, emitting C<item-requeued>) after verifying its digest
against the journal (C<CheckpointDrift> on mismatch). Reopening a B<completed>
step removes it AND every later step from C<completed-steps> and clears their
item state — downstream consumed its provides, so soundness wins over the
tempting single-step re-run — while rehydrating the reopened step's successes
from its reserved Context key so only the formerly-dead items re-run.

=head2 Failure modes

=begin table :caption<Item-step failure handling>
Situation                        | Outcome
=================================================================================================
Item attempt throws, budget left | C<item-failed>, backoff, C<item-retry>, re-run
Item exhausts its retry budget   | DLQ 'dead' record, C<item-dead-lettered>, checkpoint; run continues
Step finishes with dead items    | C<run-completed> with C<items-dead> > 0 (unless fail-on-dead)
fail-on-dead step has dead items | step checkpointed complete, then C<X::…::ItemsDead> throws
Context mutated in process-item  | the C<set> throws → the item fails (then retries/dead-letters)
Duplicate item-key at activation | run aborts (die)
Item set changed shape on resume | C<X::…::CheckpointDrift>
is-cancelled turns true          | drain in-flight, checkpoint, C<run-cancelled>, C<X::…::Cancelled>
DLQ append fails                 | fatal — the exception propagates out of the run
=end table

=head2 Cancellation latency

C<is-cancelled> is observed at the top of the inbox loop, i.e. when the next
inbox message arrives. Once observed, no further items are dispatched (including
items whose backoff timer fires — their C<wake-retry> is ignored and they stay
pending for resume), and the coordinator drains only the ≤ C<degree> already
in-flight items. If the B<only> outstanding work is a retry timer (no items in
flight), cancellation is not acted on until that timer fires and delivers its
message — so the worst-case cancel latency is roughly the pending backoff delay,
which is bounded by the item C<RetryPolicy>'s C<max-delay>.

=head1 RUN UNTIL DONE

C<method run-until-done(Plan:D, Context:D, IO::Path:D :$checkpoint-path!,
RetryPolicy:D :$run-retry = …, Bool :$retry-dead = False --> Context:D)> supervises
a pipeline to completion across B<whole-run> retries — the outer ring above per-item
and per-step retries.

=begin code :lang<raku>

my $ctx = $runner.run-until-done(
    $plan, $seed-ctx,
    checkpoint-path => 'run.checkpoint.json'.IO,
    run-retry       => LLM::Data::Pipeline::RetryPolicy.new(
        :max-attempts(5), :base-delay(30), :max-delay(600)),   # patient, minutes-scale
);

=end code

=item B<Attempt 1> is C<resume> if the checkpoint already exists (so it composes
      with an external re-invoker that restarts the process after a crash),
      otherwise C<run> with the seeded Context. Every later attempt is C<resume>.
=item B<Not retried> (rethrown immediately): plan-validation failures (checked up
      front — a malformed plan cannot be fixed by retrying),
      C<X::LLM::Data::Pipeline::Cancelled> (the caller's own intent), and
      C<X::LLM::Data::Pipeline::CheckpointDrift> (the same checkpoint will drift
      again). Also C<X::LLM::Data::Pipeline::ItemsDead> B<unless> C<:retry-dead> —
      without requeuing, a re-run reproduces the identical dead set, so retrying is
      futile.
=item B<Retried>: everything else. Each retry emits a C<run-retry> event
      (C<attempt>, C<max-attempts>, C<delay>, C<error>, C<exception>), backs off via
      C<&.schedule-after>, then C<resume>s. When the C<run-retry> budget is spent the
      B<last error is rethrown unchanged>.
=item B<:retry-dead> composes: when True, every attempt requeues dead items first,
      and C<ItemsDead> becomes retryable. Default False keeps poison items dead
      across retries so they do not burn the run-retry budget — that is what the DLQ
      is for.
=item Completing B<with> dead items is a success (C<run-completed>, C<items-dead> >
      0); only C<fail-on-dead> steps escalate that to a failure.
=item The attempt count is B<process-local> — a fresh invocation starts with a fresh
      budget; nothing about it is persisted.

=head1 OPERATIONAL RECIPES

=head2 run-until-done vs an external supervisor

C<run-until-done> retries B<in-process>: ideal for transient upstream trouble (a
flaky backend, a rate limit) where the process itself is healthy. It does not
survive the process dying. For that, pair it with an B<external supervisor>
(systemd C<Restart=on-failure>, a Kubernetes C<Job> with C<backoffLimit>, a cron
re-invoker): because attempt 1 resumes an existing checkpoint, simply
re-invoking the same command after a crash picks up exactly where it left off.
The two compose — in-process retries for transient errors, the external
supervisor for process death.

=head2 DLQ triage

Dead items are journaled next to the checkpoint as C<< <name>.dlq.jsonl >>. To
see what is currently dead (last record per item wins), with C<jq>:

=begin code :lang<bash>

# Latest record per (step,key); show only those still 'dead'.
jq -s 'group_by(.step + " " + .key)
       | map(last) | map(select(.kind == "dead"))
       | .[] | {step, key, attempts, error: .error.message}' run.dlq.jsonl

=end code

or in Raku with C<JSONL::Reader> (dedupe by last record per (step, key)):

=begin code :lang<raku>

use JSONL::Reader;
my %last;
%last{"{.<step>}\0{.<key>}"} = $_ for JSONL::Reader.new(:path($dlq)).list.map(*.value);
for %last.values.grep(*.<kind> eq 'dead') -> %d {
    say "%d<step>/%d<key>: %d<error><message> (%d<attempts> attempts)";
    with %d<inference> { say "  model summary: {.<summary>}" }   # the raw inference cause
}

=end code

=head2 Should I use :retry-dead?

=item B<Transient> item failures (backend hiccup, timeout) → C<:retry-dead> so a
      later attempt reprocesses them once the upstream recovers.
=item B<Poison> items (malformed input, a bug the model can't get past) → leave
      C<:retry-dead> off; they stay journaled for offline triage and never burn the
      run-retry budget. Fix the cause, then a one-off C<resume(:retry-dead)> drains
      them.

=head1 SEE ALSO

L<LLM::Data::Pipeline::Event> for the full event taxonomy and JSON shapes;
L<LLM::Data::Pipeline::Step::Items> for the item-step contract;
L<LLM::Data::Pipeline::RetryPolicy> and L<LLM::Data::Pipeline::Exceptions>.

=head1 AUTHOR

Matt Doughty <matt@apogee.guru>

=head1 COPYRIGHT AND LICENSE

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
