use JSON::Fast;
use LLM::Data::Pipeline::Plan;
use LLM::Data::Pipeline::Context;
use LLM::Data::Pipeline::Step;

unit class LLM::Data::Pipeline::Runner;

has &.on-step;

method run(
	LLM::Data::Pipeline::Plan:D $plan,
	LLM::Data::Pipeline::Context:D $ctx,
	IO::Path :$checkpoint-path
	--> LLM::Data::Pipeline::Context:D
) {
	$plan.validate($ctx);
	self!execute-steps($plan.steps, $ctx, :completed-steps(()), :$checkpoint-path);
	$ctx;
}

method resume(
	LLM::Data::Pipeline::Plan:D $plan,
	IO::Path:D $checkpoint-path
	--> LLM::Data::Pipeline::Context:D
) {
	die "Checkpoint file not found: $checkpoint-path" unless $checkpoint-path.e;
	my %checkpoint = from-json($checkpoint-path.slurp);
	my List $completed = %checkpoint<completed-steps>.list;
	my LLM::Data::Pipeline::Context $ctx =
		LLM::Data::Pipeline::Context.from-snapshot(%checkpoint<context>);
	$plan.validate($ctx);
	self!execute-steps($plan.steps, $ctx, :completed-steps($completed), :$checkpoint-path);
	$ctx;
}

method !execute-steps(
	@steps,
	LLM::Data::Pipeline::Context:D $ctx,
	:$completed-steps,
	:$checkpoint-path
	--> Nil
) {
	my @completed = $completed-steps.list;
	my Set $completed-set = @completed.Set;

	for @steps -> LLM::Data::Pipeline::Step $step {
		if $step.name (elem) $completed-set {
			&!on-step($step.name, 'skip') if &!on-step.defined;
			next;
		}

		&!on-step($step.name, 'start') if &!on-step.defined;
		$step.execute($ctx);
		@completed.push($step.name);
		$completed-set = @completed.Set;
		self!write-checkpoint($checkpoint-path, $ctx, @completed) if $checkpoint-path.defined;
		&!on-step($step.name, 'complete') if &!on-step.defined;
	}
}

method !write-checkpoint(
	IO::Path:D $path,
	LLM::Data::Pipeline::Context:D $ctx,
	@completed-steps
	--> Nil
) {
	my %checkpoint = %(
		completed-steps => @completed-steps.list,
		context         => $ctx.snapshot,
	);
	my IO::Path $tmp = $path.sibling($path.basename ~ '.tmp');
	$tmp.spurt(to-json(%checkpoint, :sorted-keys));
	$tmp.rename($path);
}
