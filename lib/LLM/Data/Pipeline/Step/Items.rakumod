use LLM::Data::Pipeline::Step;
use LLM::Data::Pipeline::Context;
use LLM::Data::Pipeline::RetryPolicy;

unit role LLM::Data::Pipeline::Step::Items does LLM::Data::Pipeline::Step;

#| The items to process, as an Iterable. MUST be deterministic given the
#| Context (same Context ⇒ same items in the same order) and each item MUST be
#| JSON-safe (it is digested and may be journaled). Called once per activation
#| and materialized by the Runner.
method items(LLM::Data::Pipeline::Context:D $ctx --> Iterable) { ... }

#| Stable string key for the item at 0-based index C<$i>. Defaults to the index.
#| Override to return a content-derived key for reorder-stable resume. Keys MUST
#| be unique across the item set (duplicates are detected at activation).
method item-key(Int:D $i, $item --> Str:D) { $i.Str }

#| Process a single item. Runs on a B<worker thread>: it MUST NOT mutate the
#| Context (which is frozen for the duration) and SHOULD be side-effect-free or
#| idempotent — it is B<at-least-once> (a retry re-runs it). The return value is
#| the item's result, recorded against its key and (via C<finalize>) promoted
#| into the Context. Throwing triggers the item retry policy.
method process-item(LLM::Data::Pipeline::Context:D $ctx, $item, Str:D $key --> Any) { ... }

#| Assemble this step's C<provides> from the collected item results. Runs on the
#| B<coordinator thread> after all items are terminal and the Context is thawed.
#| The results are available in the reserved Context key C<"{name}/items">
#| (key→result Hash) and dead keys in C<"{name}/dead"> (sorted List).
#|
#| MUST be B<idempotent>: a crash between C<finalize> and the step-boundary
#| checkpoint means the step is not yet recorded complete, so a resume re-runs
#| C<finalize> (over the same recovered results). Derive C<provides> purely from
#| the reserved keys / results; do not accumulate onto or mutate prior state.
method finalize(LLM::Data::Pipeline::Context:D $ctx --> Nil) { }

#| Desired worker parallelism for this step. C<0> means "use the Runner default".
method degree(--> UInt:D) { 0 }

#| Per-item retry policy. The type object means "use the Runner default".
method item-retry(--> LLM::Data::Pipeline::RetryPolicy) { LLM::Data::Pipeline::RetryPolicy }

#| If True, the run fails (X::LLM::Data::Pipeline::ItemsDead) when this step
#| finishes with any dead-lettered items. Default False: dead items are journaled
#| and the run continues.
method fail-on-dead(--> Bool:D) { False }

#| Item steps are never executed via the plain-step path; the Runner branches on
#| C<does Step::Items> and drives them through its parallel item engine.
method execute(LLM::Data::Pipeline::Context:D $ctx --> Nil) {
	die "{self.name}: a Step::Items step must be run by "
		~ "LLM::Data::Pipeline::Runner v0.4+ (its item engine), not executed "
		~ "directly.";
}

=begin pod

=head1 NAME

LLM::Data::Pipeline::Step::Items - A pipeline step whose work is a set of independently-processed items

=head1 SYNOPSIS

=begin code :lang<raku>

use LLM::Data::Pipeline::Step::Items;

class TagChunks does LLM::Data::Pipeline::Step::Items {
	method name(--> Str:D) { 'tag-chunks' }
	method description(--> Str:D) { 'Tag every story chunk' }
	method requires(--> List) { ('chunks',) }
	method provides(--> List) { ('tag-chunks/items', 'tags') }

	# Deterministic given the Context; JSON-safe items.
	method items($ctx --> Iterable) { $ctx.get('chunks').list }

	# Content-derived keys survive reordering of the chunk list.
	method item-key(Int:D $i, $chunk --> Str:D) { $chunk<id> }

	# Worker thread: no Context mutation, at-least-once.
	method process-item($ctx, $chunk, Str:D $key --> Any) {
		my $task = build-task($chunk);
		$task.on-call-complete($runner.telemetry-sink(:step<tag-chunks>, :$key));
		$task.execute;   # returns a JSON-safe result
	}

	# Coordinator thread: assemble provides from the collected results.
	method finalize($ctx --> Nil) {
		my %items = $ctx.get('tag-chunks/items');
		$ctx.set('tags', %items.values.flat.unique.sort.list);
	}
}

=end code

=head1 DESCRIPTION

A C<Step::Items> is a L<Step|LLM::Data::Pipeline::Step> whose body is a set of
items processed independently and in parallel by the Runner's item engine.
Instead of C<execute>, you implement C<items>, C<process-item>, and (usually)
C<finalize>. The Runner branches on C<does Step::Items> and never calls
C<execute> (it dies if you do).

=head2 The contract

=item B<Determinism.> C<items($ctx)> must return the same items in the same
      order for the same Context. It is called once per activation and
      materialized; a resumed in-progress step recomputes it and compares a
      fingerprint (count + SHA-256 of the keys) against the checkpoint, throwing
      C<X::LLM::Data::Pipeline::CheckpointDrift> on a mismatch.

=item B<JSON-safe items.> Each item is digested (SHA-256 of its JSON) for the
      dead-letter journal and must round-trip through JSON.

=item B<Unique keys.> C<item-key($i, $item)> must be unique across the set.
      Duplicates are detected at activation and abort the run. The default key
      is the 0-based index (stable only if the list order is stable); override
      with a content-derived key for reorder-stable resume.

=item B<No Context mutation in C<process-item>.> It runs on a worker thread
      while the Context is frozen; calling C<set> dies. Reads are safe — but
      C<get> returns the B<live reference>, so mutating a fetched value in place
      (pushing to a fetched Array, assigning into a fetched Hash) is NOT caught
      by the frozen flag and is a data race under parallelism — B<undefined
      behavior>. Treat everything you read as read-only; assemble all outputs in
      C<finalize>.

=item B<At-least-once.> C<process-item> may run more than once for a key (retry,
      or resume after a crash between processing and checkpoint). Recording is
      exactly-once per checkpoint lineage, so a B<pure> C<process-item> is
      effectively exactly-once; anything with external side effects must be
      idempotent.

=item B<Idempotent C<finalize>.> A crash between C<finalize> and the
      step-boundary checkpoint leaves the step not-yet-complete, so a resume
      re-runs C<finalize> over the recovered results. It must derive C<provides>
      purely from the reserved keys / results, never accumulate onto prior state.

=head2 Reserved Context keys

Before C<finalize> the engine writes two reserved keys into the (thawed)
Context, so C<finalize> and downstream steps can read them — declare whichever
you consume in C<provides>:

=item C<"{name}/items"> — Hash of item key → result (successful items only).
=item C<"{name}/dead"> — sorted List of dead-lettered item keys.

=head1 SEE ALSO

L<LLM::Data::Pipeline::Runner> for the parallel engine, checkpoint v2, and DLQ;
L<LLM::Data::Pipeline::RetryPolicy> for per-item retries.

=head1 AUTHOR

Matt Doughty <matt@apogee.guru>

=head1 COPYRIGHT AND LICENSE

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
