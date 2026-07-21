unit class LLM::Data::Pipeline::RetryPolicy;

#| Maximum number of attempts (not retries): C<1> means "try once, never retry".
has UInt:D $.max-attempts = 3;
#| First backoff delay in seconds; doubles each subsequent attempt.
has Real:D $.base-delay = 5.0;
#| Ceiling on the exponential part of the backoff, in seconds.
has Real:D $.max-delay = 120.0;
#| Random jitter added on top of the (capped) exponential delay, in seconds.
has Real:D $.jitter = 0.5;

#| Backoff before the C<$attempt>-th retry (1-based): the exponential term
#| C<base-delay * 2 ** (attempt - 1)>, capped at C<max-delay>, plus up to
#| C<jitter> seconds of uniform noise. With C<jitter> at 0 the result is
#| deterministic.
method delay-for(UInt:D $attempt --> Real:D) {
	my Real:D $exponential = $!base-delay * 2 ** ($attempt - 1);
	my Real:D $capped = $exponential min $!max-delay;
	$capped + rand * $!jitter;
}

#| True once C<$attempts> attempts have been made and no budget remains.
method exhausted(UInt:D $attempts --> Bool:D) {
	$attempts >= $!max-attempts;
}

=begin pod

=head1 NAME

LLM::Data::Pipeline::RetryPolicy - Exponential-backoff retry budget for pipeline steps

=head1 SYNOPSIS

=begin code :lang<raku>

use LLM::Data::Pipeline::RetryPolicy;

my $policy = LLM::Data::Pipeline::RetryPolicy.new(
	max-attempts => 4,      # try up to 4 times
	base-delay   => 2.0,    # 2s, then 4s, then 8s …
	max-delay    => 60.0,   # … capped at 60s
	jitter       => 0.5,    # plus up to 0.5s of noise
);

$policy.delay-for(1);       # ~2.0  (2 * 2**0 + jitter)
$policy.delay-for(2);       # ~4.0
$policy.delay-for(3);       # ~8.0
$policy.exhausted(3);       # False — one attempt left
$policy.exhausted(4);       # True

# Wire it into a Runner as the default policy for every plain step:
my $runner = LLM::Data::Pipeline::Runner.new(:step-retry($policy));

=end code

=head1 DESCRIPTION

A C<RetryPolicy> is a small value object describing how many times a step may be
attempted and how long to wait between attempts. It is intentionally simple and
side-effect-free: C<delay-for> and C<exhausted> are the only behavior.

=head2 Deliberately slower than the inference layer

This is the B<second> retry layer. Beneath a pipeline step, an
C<LLM::Data::Inference::Task> already runs its own per-call backoff and backend
fallback chain, retrying fast against alternate models. By the time a failure
escapes I<that> chain and reaches the step, the fast-retry budget is spent — so
the Runner's policy defaults to a much longer backoff (C<base-delay> of 5s
rather than sub-second) to avoid hammering an upstream that has already
signalled sustained trouble. Treat it as the outer, patient ring around the
inner, eager one.

=head2 The backoff formula

=begin code :lang<raku>

delay-for(attempt) = min(max-delay, base-delay * 2 ** (attempt - 1)) + rand * jitter

=end code

C<attempt> is 1-based (the delay I<before> the first retry is C<delay-for(1)>).
The cap applies only to the exponential term; jitter is added afterwards, so a
result may slightly exceed C<max-delay>. Set C<jitter> to C<0> for a fully
deterministic schedule (useful in tests with a virtual clock).

=head1 AUTHOR

Matt Doughty <matt@apogee.guru>

=head1 COPYRIGHT AND LICENSE

Copyright 2026 Matt Doughty

This library is free software; you can redistribute it and/or modify it under the Artistic License 2.0.

=end pod
