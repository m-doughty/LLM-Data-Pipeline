use LLM::Data::Pipeline::Step;
use LLM::Data::Pipeline::Step::Items;
use LLM::Data::Pipeline::Context;
use LLM::Data::Pipeline::RetryPolicy;

unit module PipelineTestKit;

#| A scripted Step::Items fixture. Each item's behavior is driven by a per-key
#| script Hash; a Lock guards all mutable bookkeeping so the fixture itself is
#| never the source of a data race (the engine runs process-item on worker
#| threads). Supported script kinds:
#|   { kind => 'succeed' }                         → returns "ok-$key"
#|   { kind => 'die-always' }                      → always throws
#|   { kind => 'flaky', fails => N }               → throws N times, then succeeds
#|   { kind => 'block-on', promise => $p }         → awaits $p, then succeeds
#|   { kind => 'block-then-die', promise => $p }   → awaits $p, then throws
#|   { kind => 'sub-fail', units => N,
#|     fail-at => K, fail-times => M }             → N sequential SUB-UNITS,
#|       saving a partial after each completed one via C<$.runner>'s
#|       partial-sink; the sub-unit at index K throws on the first M attempts
#|       that reach it. The result is the joined sub-unit log, so a resumed
#|       item's result proves which sub-units it carried over.
#| A missing script entry defaults to 'succeed'.
class ScriptedItemStep does LLM::Data::Pipeline::Step::Items is export {
	has Str:D  $.step-name is required;
	has        @.item-list is required;
	has        %.script;
	has Bool:D $.content-keys = False;
	has        @.provides-list;
	has Bool:D $.fails-on-dead = False;
	has UInt:D $.step-degree = 0;
	has LLM::Data::Pipeline::RetryPolicy $.step-item-retry;
	has        &.finalizer;
	has Lock   $.lock = Lock.new;
	has        %!exec-count;
	#| The Runner driving this step, needed to mint a partial-sink. Settable
	#| after construction because the Runner and the step are built in either
	#| order (and a step under direct unit test has none at all).
	has        $.runner is rw;
	has        %!received-partial;   # key => the :%partial the last attempt saw
	has        %!sub-exec;           # key => sub-units actually executed
	has        %!sub-fail-count;     # "key/unit" => times that unit has thrown

	method name(--> Str:D) { $!step-name }
	method description(--> Str:D) { "scripted $!step-name" }
	method provides(--> List) {
		(@!provides-list ?? @!provides-list !! ("$!step-name/items",)).list
	}
	method items(LLM::Data::Pipeline::Context:D $ctx --> Iterable) { @!item-list }
	method item-key(Int:D $i, $item --> Str:D) {
		$!content-keys ?? $item.Str !! $i.Str
	}
	method degree(--> UInt:D) { $!step-degree }
	method fail-on-dead(--> Bool:D) { $!fails-on-dead }
	method item-retry(--> LLM::Data::Pipeline::RetryPolicy) {
		$!step-item-retry // LLM::Data::Pipeline::RetryPolicy
	}

	#| Number of times process-item was invoked for C<$key> (across attempts).
	method exec-count-for(Str:D $key --> Int:D) {
		$!lock.protect({ %!exec-count{$key} // 0 })
	}
	#| All keys that were ever executed.
	method executed-keys(--> List) {
		$!lock.protect({ %!exec-count.keys.sort.list })
	}

	#| The C<:%partial> the most recent C<process-item> for C<$key> was handed
	#| (C<%()> when it was handed none, C<Nil> when the key never ran).
	method received-partial-for(Str:D $key) {
		$!lock.protect({ %!received-partial{$key} })
	}

	#| How many SUB-UNITS were actually executed for C<$key> across every
	#| attempt — the number a resume must not inflate.
	method sub-exec-count-for(Str:D $key --> Int:D) {
		$!lock.protect({ %!sub-exec{$key} // 0 })
	}

	method process-item(
		LLM::Data::Pipeline::Context:D $ctx, $item, Str:D $key, :%partial --> Any
	) {
		my Int $n = $!lock.protect({
			%!received-partial{$key} = %partial.Hash;
			++%!exec-count{$key};
		});
		my %s = %!script{$key} // %( kind => 'succeed' );
		given %s<kind> {
			when 'succeed'    { return "ok-$key" }
			when 'die-always' { die "die-always: $key" }
			when 'flaky' {
				die "flaky-fail: $key (attempt $n)" if $n <= %s<fails>;
				return "ok-$key";
			}
			when 'block-on' {
				await %s<promise>;
				return "ok-$key";
			}
			when 'block-then-die' {
				await %s<promise>;
				die "blocked-die: $key";
			}
			when 'sub-fail' {
				my Int $units      = (%s<units>      // 3).Int;
				my Int $fail-at    = (%s<fail-at>    // -1).Int;
				my Int $fail-times = (%s<fail-times> //  1).Int;
				# The sink is minted per attempt, exactly as a real step does
				# it: the Runner binds the active inbox at mint time.
				my &save = $!runner.defined
					?? $!runner.partial-sink(:step($!step-name), :$key)
					!! Callable;
				# Resume from the partial. A partial whose shape we do not
				# recognise reads as a fresh start, never a death.
				my $raw-next = %partial<next>;
				my Int $start = ($raw-next ~~ Int:D) ?? $raw-next.Int !! 0;
				$start = 0 if $start < 0 || $start > $units;
				my @log = ((%partial<log> // []).list.map(*.Str));
				@log = () if $start == 0;
				for $start ..^ $units -> $u {
					if $u == $fail-at {
						my Int $f = $!lock.protect({
							++%!sub-fail-count{"$key/$u"};
						});
						die "sub-fail: $key unit $u (failure $f)"
							if $f <= $fail-times;
					}
					$!lock.protect({ ++%!sub-exec{$key} });
					@log.push("$key-$u");
					&save(%( next => $u + 1, log => @log.Array ))
						if &save.defined;
				}
				return @log.join(',');
			}
			default { return "ok-$key" }
		}
	}

	method finalize(LLM::Data::Pipeline::Context:D $ctx --> Nil) {
		&!finalizer.($ctx) if &!finalizer.defined;
	}
}

#| A plain (non-items) step that sets one Context key — for building multi-step
#| plans around item steps.
class SetKeyStep does LLM::Data::Pipeline::Step is export {
	has Str:D $.step-name is required;
	has Str:D $.key is required;
	has       $.val is required;
	method name(--> Str:D) { $!step-name }
	method description(--> Str:D) { "sets $!key" }
	method provides(--> List) { ($!key,) }
	method execute(LLM::Data::Pipeline::Context:D $ctx --> Nil) { $ctx.set($!key, $!val); }
}

#| A plain step that fails its first C<$.fails> executions (per instance, across
#| run-until-done attempts) then succeeds — for exercising whole-run retries. Its
#| C<exec-count> is the number of times execute ran.
class FlakyPlainStep does LLM::Data::Pipeline::Step is export {
	has Str:D $.step-name is required;
	has Int:D $.fails is required;
	has       @.requires-list;
	has Str   $.out-key;
	has Int   $.exec-count is rw = 0;
	submethod TWEAK { $!out-key //= $!step-name ~ '-out'; }
	method name(--> Str:D) { $!step-name }
	method description(--> Str:D) { "flaky plain $!step-name" }
	method requires(--> List) { @!requires-list.list }
	method provides(--> List) { ($!out-key,) }
	method execute(LLM::Data::Pipeline::Context:D $ctx --> Nil) {
		$!exec-count++;
		die "$!step-name fails (execution $!exec-count)" if $!exec-count <= $!fails;
		$ctx.set($!out-key, "ok-$!exec-count");
	}
}

#| A schedule-after seam that records every requested delay and fires the
#| callback synchronously (no real sleep). Returns C<(&seam, @delays)>.
sub instant-timer(--> List) is export {
	my @delays;
	my &seam = -> Real $s, &cb { @delays.push($s); cb(Promise.kept); Promise.kept };
	(&seam, @delays);
}

#| A small deterministic splitmix64-style PRNG so scripted "random" failure
#| patterns are reproducible (no reliance on the global RNG).
class SeededRng is export {
	has uint64 $.state;
	submethod BUILD(UInt:D :$seed = 0x9e3779b97f4a7c15) { $!state = $seed; }
	method next-u64(--> UInt:D) {
		$!state = ($!state + 0x9e3779b97f4a7c15) +& 0xFFFFFFFFFFFFFFFF;
		my uint64 $z = $!state;
		$z = (($z +^ ($z +> 30)) * 0xbf58476d1ce4e5b9) +& 0xFFFFFFFFFFFFFFFF;
		$z = (($z +^ ($z +> 27)) * 0x94d049bb133111eb) +& 0xFFFFFFFFFFFFFFFF;
		($z +^ ($z +> 31)) +& 0xFFFFFFFFFFFFFFFF;
	}
	#| Uniform integer in [0, $n).
	method below(UInt:D $n --> UInt:D) { self.next-u64 % $n }
}
