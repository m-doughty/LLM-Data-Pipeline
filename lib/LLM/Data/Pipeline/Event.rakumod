#| Base role shared by every pipeline event. Every event carries the three
#| envelope fields (C<seq>, C<at>, C<run-id>) and knows how to render itself as
#| a JSON-safe Hash. Concrete event classes add their own payload attributes and
#| override C<kind> and C<payload>.
role LLM::Data::Pipeline::Event {
	#| Strictly increasing per-run sequence number (starts at 1).
	has UInt:D $.seq is required;
	#| Wall-clock Instant the event was emitted.
	has Instant:D $.at is required;
	#| Identifier of the run that produced this event.
	has Str:D $.run-id is required;

	#| Stable kebab-case discriminator, e.g. C<'step-started'>.
	method kind(--> Str:D) { ... }

	#| Event-specific fields as a JSON-safe Hash. Overridden per class.
	method payload(--> Hash:D) { {} }

	#| Full JSON-safe Hash: envelope fields merged with the payload. C<at> is
	#| serialized as a POSIX epoch-seconds number (fractional).
	method to-hash(--> Hash:D) {
		my %h = self.payload;
		%h<kind>   = self.kind;
		%h<seq>    = $!seq;
		%h<run-id> = $!run-id;
		%h<at>     = $!at.to-posix[0].Num;
		%h;
	}
}

# ---------------------------------------------------------------------------
# Stage 2 — emitted now
# ---------------------------------------------------------------------------

#| A run has passed validation and is about to execute its steps.
class LLM::Data::Pipeline::Event::RunStarted does LLM::Data::Pipeline::Event {
	has UInt:D $.plan-size is required;
	has Bool:D $.resumed is required;
	has Str    $.checkpoint-path;
	method kind(--> Str:D) { 'run-started' }
	method payload(--> Hash:D) {
		{
			plan-size       => $!plan-size,
			resumed         => $!resumed,
			checkpoint-path => $!checkpoint-path,
		}
	}
}

#| A step was skipped because a resumed checkpoint marks it complete.
class LLM::Data::Pipeline::Event::StepSkipped does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has UInt:D $.ordinal is required;
	has UInt:D $.total is required;
	has Str:D  $.description is required;
	method kind(--> Str:D) { 'step-skipped' }
	method payload(--> Hash:D) {
		{
			step        => $!step,
			ordinal     => $!ordinal,
			total       => $!total,
			description => $!description,
		}
	}
}

#| A step is about to execute.
class LLM::Data::Pipeline::Event::StepStarted does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has UInt:D $.ordinal is required;
	has UInt:D $.total is required;
	has Str:D  $.description is required;
	method kind(--> Str:D) { 'step-started' }
	method payload(--> Hash:D) {
		{
			step        => $!step,
			ordinal     => $!ordinal,
			total       => $!total,
			description => $!description,
		}
	}
}

#| A step finished executing successfully.
class LLM::Data::Pipeline::Event::StepCompleted does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has UInt:D $.ordinal is required;
	has UInt:D $.total is required;
	has Str:D  $.description is required;
	has Real:D $.duration is required;
	#| Item tallies — present only for Step::Items steps (undefined for plain steps).
	has UInt   $.items-done;
	has UInt   $.items-dead;
	method kind(--> Str:D) { 'step-completed' }
	method payload(--> Hash:D) {
		my %p =
			step        => $!step,
			ordinal     => $!ordinal,
			total       => $!total,
			description => $!description,
			duration    => $!duration;
		%p<items-done> = $!items-done if $!items-done.defined;
		%p<items-dead> = $!items-dead if $!items-dead.defined;
		%p;
	}
}

#| A checkpoint file was written to disk.
class LLM::Data::Pipeline::Event::CheckpointWritten does LLM::Data::Pipeline::Event {
	has Str:D $.path is required;
	has Str:D $.trigger is required;
	method kind(--> Str:D) { 'checkpoint-written' }
	method payload(--> Hash:D) {
		{ path => $!path, trigger => $!trigger }
	}
}

#| The run finished: every step is either completed or skipped.
class LLM::Data::Pipeline::Event::RunCompleted does LLM::Data::Pipeline::Event {
	has Real:D $.duration is required;
	has UInt:D $.steps-run is required;
	has UInt:D $.steps-skipped is required;
	has UInt:D $.items-done is required;
	has UInt:D $.items-dead is required;
	method kind(--> Str:D) { 'run-completed' }
	method payload(--> Hash:D) {
		{
			duration      => $!duration,
			steps-run     => $!steps-run,
			steps-skipped => $!steps-skipped,
			items-done    => $!items-done,
			items-dead    => $!items-dead,
		}
	}
}

#| A step threw and aborted the run; the exception is rethrown unchanged.
class LLM::Data::Pipeline::Event::RunFailed does LLM::Data::Pipeline::Event {
	has Str    $.step;
	has Str:D  $.error is required;
	has Str:D  $.exception is required;
	method kind(--> Str:D) { 'run-failed' }
	method payload(--> Hash:D) {
		{ step => $!step, error => $!error, exception => $!exception }
	}
}

# ---------------------------------------------------------------------------
# Stage 3 — emitted from 0.3.0
# ---------------------------------------------------------------------------

#| A step attempt failed. C<will-retry> says whether another attempt follows;
#| when True, C<retry-delay> is the backoff (seconds) before it. When False the
#| step's retry budget is spent and the run aborts (C<retry-delay> is 0).
class LLM::Data::Pipeline::Event::StepFailed does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has UInt:D $.attempt is required;
	has Str:D  $.error is required;
	has Str:D  $.exception is required;
	has Bool:D $.will-retry is required;
	has Real:D $.retry-delay is required;
	method kind(--> Str:D) { 'step-failed' }
	method payload(--> Hash:D) {
		{
			step        => $!step,
			attempt     => $!attempt,
			error       => $!error,
			exception   => $!exception,
			will-retry  => $!will-retry,
			retry-delay => $!retry-delay,
		}
	}
}

#| A failed step is being retried after its backoff. C<attempt> is the number of
#| the upcoming attempt; C<delay> is the backoff that was waited.
class LLM::Data::Pipeline::Event::StepRetry does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has UInt:D $.attempt is required;
	has Real:D $.delay is required;
	has Str:D  $.error is required;
	method kind(--> Str:D) { 'step-retry' }
	method payload(--> Hash:D) {
		{
			step    => $!step,
			attempt => $!attempt,
			delay   => $!delay,
			error   => $!error,
		}
	}
}

# ---------------------------------------------------------------------------
# Stage 4 — emitted from 0.4.0
# ---------------------------------------------------------------------------

#| A single item within a Step::Items step started processing.
class LLM::Data::Pipeline::Event::ItemStarted does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has Str:D  $.key is required;
	has UInt:D $.attempt is required;
	method kind(--> Str:D) { 'item-started' }
	method payload(--> Hash:D) {
		{ step => $!step, key => $!key, attempt => $!attempt }
	}
}

#| A single item finished processing successfully.
class LLM::Data::Pipeline::Event::ItemCompleted does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has Str:D  $.key is required;
	has UInt:D $.attempt is required;
	has Real:D $.duration is required;
	method kind(--> Str:D) { 'item-completed' }
	method payload(--> Hash:D) {
		{
			step     => $!step,
			key      => $!key,
			attempt  => $!attempt,
			duration => $!duration,
		}
	}
}

#| A single item attempt failed and may be retried.
class LLM::Data::Pipeline::Event::ItemFailed does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has Str:D  $.key is required;
	has UInt:D $.attempt is required;
	has Str:D  $.error is required;
	has Str:D  $.exception is required;
	method kind(--> Str:D) { 'item-failed' }
	method payload(--> Hash:D) {
		{
			step      => $!step,
			key       => $!key,
			attempt   => $!attempt,
			error     => $!error,
			exception => $!exception,
		}
	}
}

#| An item exhausted its retries and was dead-lettered.
class LLM::Data::Pipeline::Event::ItemDeadLettered does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has Str:D  $.key is required;
	has UInt:D $.attempts is required;
	has Str:D  $.error is required;
	method kind(--> Str:D) { 'item-dead-lettered' }
	method payload(--> Hash:D) {
		{
			step     => $!step,
			key      => $!key,
			attempts => $!attempts,
			error    => $!error,
		}
	}
}

#| A previously dead-lettered item was requeued (via resume :retry-dead).
class LLM::Data::Pipeline::Event::ItemRequeued does LLM::Data::Pipeline::Event {
	has Str:D $.step is required;
	has Str:D $.key is required;
	method kind(--> Str:D) { 'item-requeued' }
	method payload(--> Hash:D) {
		{ step => $!step, key => $!key }
	}
}

#| Progress snapshot for a Step::Items step, emitted after every item terminal
#| transition. C<pending> = total − done − dead − in-flight (items awaiting
#| dispatch or a retry timer).
class LLM::Data::Pipeline::Event::Progress does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has UInt:D $.done is required;
	has UInt:D $.dead is required;
	has UInt:D $.total is required;
	has UInt:D $.in-flight is required;
	has UInt:D $.pending is required;
	method kind(--> Str:D) { 'progress' }
	method payload(--> Hash:D) {
		{
			step      => $!step,
			done      => $!done,
			dead      => $!dead,
			total     => $!total,
			in-flight => $!in-flight,
			pending   => $!pending,
		}
	}
}

#| The run was cancelled cooperatively and drained.
class LLM::Data::Pipeline::Event::RunCancelled does LLM::Data::Pipeline::Event {
	has Str    $.step;
	has UInt:D $.items-done is required;
	has UInt:D $.items-dead is required;
	method kind(--> Str:D) { 'run-cancelled' }
	method payload(--> Hash:D) {
		{
			step       => $!step,
			items-done => $!items-done,
			items-dead => $!items-dead,
		}
	}
}

#| Opaque telemetry forwarded from a step's inference layer via telemetry-sink.
class LLM::Data::Pipeline::Event::Telemetry does LLM::Data::Pipeline::Event {
	has Str:D  $.step is required;
	has Str    $.key;
	has Str:D  $.stage is required;
	has        %.data;
	method kind(--> Str:D) { 'telemetry' }
	method payload(--> Hash:D) {
		{
			step  => $!step,
			key   => $!key,
			stage => $!stage,
			data  => %!data,
		}
	}
}

# ---------------------------------------------------------------------------
# Stage 5 — emitted from 0.5.0
# ---------------------------------------------------------------------------

#| A whole run is being retried by run-until-done supervision after a transient
#| failure. C<attempt> is the attempt that just failed (1-based); C<delay> is the
#| backoff before the next attempt. This is a B<supervision> event that lives
#| outside any single run's sequence: it carries C<seq> 0 (see the Runner Pod)
#| and is delivered through the C<&.on-event> callback.
class LLM::Data::Pipeline::Event::RunRetry does LLM::Data::Pipeline::Event {
	has UInt:D $.attempt is required;
	has UInt:D $.max-attempts is required;
	has Real:D $.delay is required;
	has Str:D  $.error is required;
	has Str:D  $.exception is required;
	method kind(--> Str:D) { 'run-retry' }
	method payload(--> Hash:D) {
		{
			attempt      => $!attempt,
			max-attempts => $!max-attempts,
			delay        => $!delay,
			error        => $!error,
			exception    => $!exception,
		}
	}
}

=begin pod

=head1 NAME

LLM::Data::Pipeline::Event - Typed, immutable events emitted by the pipeline Runner

=head1 SYNOPSIS

=begin code :lang<raku>

use LLM::Data::Pipeline::Event;
use LLM::Data::Pipeline::Runner;

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
$runner.run($plan, $ctx);

# Every event is also a JSON-safe Hash, ready for a log line:
$runner.events.tap(-> $e { say to-json($e.to-hash) });

=end code

=head1 DESCRIPTION

Every observable moment in a run is an immutable object composing the
C<LLM::Data::Pipeline::Event> role. The role supplies three envelope fields —

=item C<UInt $.seq> — strictly increasing per-run sequence, starting at C<1>.
=item C<Instant $.at> — wall-clock time the event was emitted.
=item C<Str $.run-id> — the run that produced it (stable within a run).

— plus two methods: C<kind> (a stable kebab-case string discriminator) and
C<to-hash> (a JSON-safe Hash of every field). In C<to-hash>, C<at> is rendered
as a POSIX epoch-seconds number (fractional), so the whole hash round-trips
cleanly through C<JSON::Fast>:

=begin code :lang<raku>

my %h = $event.to-hash;
my $json = to-json(%h);          # never throws on a Stage-2 event
my %back = from-json($json);     # %back<kind>, %back<seq>, %back<run-id>, %back<at>, …

=end code

=head1 EVENT TAXONOMY

Every kind in the taxonomy is emitted as of this release (0.5.0). Earlier
releases emitted a growing subset; the C<Status> column records when each kind
first shipped.

Nearly all events carry a per-run C<seq> starting at C<1>. The sole exception is
C<run-retry>, a B<supervision> event emitted by C<run-until-done> B<between>
inner run/resume attempts: it carries C<seq> C<0> to mark that it sits outside
any single run's totally-ordered sequence, and is delivered via C<&.on-event>
(each inner run's C<events()> Supply has already ended in C<done>).

=begin table :caption<Pipeline event taxonomy>
Kind                 | Class             | Payload fields                                          | Status
=====================================================================================================================
run-started          | RunStarted        | plan-size, resumed, checkpoint-path                     | emitted (0.2.0)
step-skipped         | StepSkipped       | step, ordinal, total, description                       | emitted (0.2.0)
step-started         | StepStarted       | step, ordinal, total, description                       | emitted (0.2.0)
step-completed       | StepCompleted     | step, ordinal, total, description, duration, items-done?, items-dead? | emitted (0.2.0)
checkpoint-written   | CheckpointWritten | path, trigger                                           | emitted (0.2.0)
run-completed        | RunCompleted      | duration, steps-run, steps-skipped, items-done, items-dead | emitted (0.2.0)
run-failed           | RunFailed         | step, error, exception                                  | emitted (0.2.0)
step-failed          | StepFailed        | step, attempt, error, exception, will-retry, retry-delay | emitted (0.3.0)
step-retry           | StepRetry         | step, attempt, delay, error                             | emitted (0.3.0)
item-started         | ItemStarted       | step, key, attempt                                      | emitted (0.4.0)
item-completed       | ItemCompleted     | step, key, attempt, duration                            | emitted (0.4.0)
item-failed          | ItemFailed        | step, key, attempt, error, exception                    | emitted (0.4.0)
item-dead-lettered   | ItemDeadLettered  | step, key, attempts, error                              | emitted (0.4.0)
item-requeued        | ItemRequeued      | step, key                                               | emitted (0.4.0)
progress             | Progress          | step, done, dead, total, in-flight, pending             | emitted (0.4.0)
run-cancelled        | RunCancelled      | step, items-done, items-dead                            | emitted (0.4.0)
telemetry            | Telemetry         | step, key, stage, data                                  | emitted (0.4.0)
run-retry            | RunRetry          | attempt, max-attempts, delay, error, exception          | emitted (0.5.0)
=end table

The C<step-completed> C<items-done>/C<items-dead> fields are present only for
C<Step::Items> steps (omitted for plain steps). C<checkpoint-written> C<trigger>
is one of C<'step'>, C<'item'>, C<'dead-letter'>, or C<'cancel'>.

All C<duration> and C<delay> fields are seconds as a number; C<checkpoint-path>
is C<Str> and may be the (undefined) type object when no checkpoint path was
given, which serializes to JSON C<null>.

=head1 SEE ALSO

L<LLM::Data::Pipeline::Runner> for how and when these events are emitted and the
per-run ordering guarantees.

=head1 AUTHOR

Matt Doughty <matt@apogee.guru>

=head1 COPYRIGHT AND LICENSE

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
