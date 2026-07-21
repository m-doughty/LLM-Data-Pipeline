=begin pod

=head1 NAME

LLM::Data::Pipeline::Exceptions - typed failures thrown by the pipeline Runner

=head1 DESCRIPTION

C<X::LLM::Data::Pipeline::StepExhausted> is thrown by
L<LLM::Data::Pipeline::Runner> when a plain step's retry budget
(L<LLM::Data::Pipeline::RetryPolicy>) is spent without a successful attempt. It
carries the step name, the number of attempts made, and the C<last-error> — the
actual exception from the final attempt — so callers can inspect (or rethrow)
the underlying cause while still catching a Pipeline-typed failure. Its
C<message> quotes the last error, so string-matching callers keep working.

B<Only thrown when retry is opted into> (C<max-attempts> greater than 1). With
the default single-attempt policy the Runner rethrows the step's original
exception unchanged, preserving the pre-0.3.0 contract that domain exception
types (e.g. C<X::LLM::Data::Inference::Exhausted>) flow straight out to callers.

The remaining classes are declared now and thrown from later releases:

=item C<X::LLM::Data::Pipeline::Cancelled> — cooperative cancellation (from 0.4.0).
=item C<X::LLM::Data::Pipeline::ItemsDead> — a C<fail-on-dead> item step finished
      with dead items (from 0.4.0).
=item C<X::LLM::Data::Pipeline::CheckpointDrift> — a resumed in-progress item step
      no longer matches its recorded fingerprint (from 0.4.0).

=head1 EXAMPLES

=begin code :lang<raku>

use LLM::Data::Pipeline::Runner;
use LLM::Data::Pipeline::Exceptions;

my $runner = LLM::Data::Pipeline::Runner.new(
	step-retry => LLM::Data::Pipeline::RetryPolicy.new(:max-attempts(3)),
);

{
	CATCH {
		when X::LLM::Data::Pipeline::StepExhausted {
			note "step {.step} gave up after {.attempts} tries";
			note "underlying cause: {.last-error.^name}: {.last-error.message}";
		}
	}
	$runner.run($plan, $ctx);
}

=end code

=end pod

unit module LLM::Data::Pipeline::Exceptions;

#|( Thrown when a plain step's retry budget is exhausted (only when
    C<max-attempts> > 1). Carries:
      * step       — the failing step's name
      * attempts   — how many attempts were made before giving up
      * last-error — the exception thrown by the final attempt
    C<message> quotes the last error so C<.message>-matching callers still
    see the underlying failure. )
class X::LLM::Data::Pipeline::StepExhausted is Exception is export {
	has Str:D $.step is required;
	has UInt:D $.attempts is required;
	has $.last-error is required;

	method message(--> Str) {
		"Step '$!step' exhausted after $!attempts attempt(s): "
			~ ($!last-error.defined ?? $!last-error.message !! 'unknown error');
	}
}

#|( RESERVED (0.4.0): thrown when a run is cancelled cooperatively via the
    Runner's C<is-cancelled> hook. Deliberately NOT a StepExhausted subclass:
    cancellation is the caller's own intent, not a failure of the pipeline. )
class X::LLM::Data::Pipeline::Cancelled is Exception is export {
	has Str $.step;

	method message(--> Str) {
		'LLM::Data::Pipeline::Runner: cancelled by caller'
			~ ($!step.defined ?? " during step '$!step'" !! '');
	}
}

#|( RESERVED (0.4.0): thrown when a C<fail-on-dead> item step finishes with one
    or more dead-lettered items. C<dead-keys> lists the item keys that never
    succeeded. )
class X::LLM::Data::Pipeline::ItemsDead is Exception is export {
	has Str:D $.step is required;
	has @.dead-keys;

	method message(--> Str) {
		"Step '$!step' completed with {@!dead-keys.elems} dead item(s): "
			~ @!dead-keys.join(', ');
	}
}

#|( RESERVED (0.4.0): thrown when a resumed in-progress item step's recomputed
    fingerprint does not match the one stored in the checkpoint — the item set
    changed shape since the checkpoint was written. Carries both fingerprints so
    the remedy ("re-run --fresh") can show the drift. )
class X::LLM::Data::Pipeline::CheckpointDrift is Exception is export {
	has Str:D $.step is required;
	has $.expected-fingerprint is required;
	has $.got is required;

	method message(--> Str) {
		"Step '$!step': checkpoint drift — item set changed since the "
			~ "checkpoint was written; re-run with a fresh checkpoint to proceed.";
	}
}
