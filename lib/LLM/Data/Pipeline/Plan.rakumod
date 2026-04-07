use LLM::Data::Pipeline::Step;
use LLM::Data::Pipeline::Context;

unit class LLM::Data::Pipeline::Plan;

has LLM::Data::Pipeline::Step @!steps;

method add-step(LLM::Data::Pipeline::Step:D $step --> Nil) {
	@!steps.push($step);
}

method steps(--> List) {
	@!steps.list;
}

method validate(LLM::Data::Pipeline::Context $ctx? --> Bool:D) {
	my Str @errors;

	# Check for duplicate step names
	my %seen;
	for @!steps -> LLM::Data::Pipeline::Step $step {
		my Str:D $name = $step.name;
		if %seen{$name}:exists {
			@errors.push: "Duplicate step name '$name'";
		}
		%seen{$name} = True;
	}

	# Build available key set from initial context
	my SetHash $available .= new;
	if $ctx.defined {
		$available.set($_) for $ctx.keys;
	}

	# Walk steps, checking requires against available keys
	for @!steps -> LLM::Data::Pipeline::Step $step {
		for @($step.requires) -> Str $key {
			unless $available{$key} {
				@errors.push: "Step '{$step.name}': missing required key '$key'";
			}
		}
		$available.set($_) for @($step.provides);
	}

	if @errors.elems > 0 {
		die "Plan validation failed:\n" ~ @errors.map({ "  - $_" }).join("\n");
	}

	True;
}
