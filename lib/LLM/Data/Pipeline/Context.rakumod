use JSON::Fast;

unit class LLM::Data::Pipeline::Context;

has %!data;
has Bool:D $!frozen = False;

method set(Str:D $key, $value --> Nil) {
	die "Context is frozen: a step must not mutate the Context while its items "
		~ "are being processed (key '$key'). process-item runs on worker threads "
		~ "and must be side-effect-free with respect to the Context; assemble "
		~ "outputs in finalize instead."
		if $!frozen;
	%!data{$key} = $value;
}

#| Freeze the Context: further C<set> calls die until C<thaw>. The Runner freezes
#| the Context for the duration of an item step's parallel processing so a stray
#| worker-thread mutation surfaces as a loud contract error rather than a
#| data-race heisenbug. Concurrent reads of the (unmutated) data are safe.
method freeze(--> Nil) { $!frozen = True; }

#| Reverse of C<freeze>: allow C<set> again (the Runner thaws before C<finalize>).
method thaw(--> Nil) { $!frozen = False; }

#| Whether the Context is currently frozen.
method frozen(--> Bool:D) { $!frozen; }

#| Return the value stored under C<$key> (or C<Any> if absent). NOTE: this is the
#| B<live reference>, not a copy — the C<frozen> flag only guards C<set>, so
#| mutating a fetched container in place (e.g. C<< $ctx.get('list').push(...) >>)
#| bypasses it entirely. Doing so from a worker thread (inside C<process-item>,
#| where the Context is frozen) is a data race and B<undefined behavior>; treat
#| fetched values as read-only there.
method get(Str:D $key --> Any) {
	%!data{$key};
}

method has(Str:D $key --> Bool:D) {
	%!data{$key}:exists;
}

method keys(--> List) {
	%!data.keys.sort.list;
}

method snapshot(--> Hash:D) {
	from-json(to-json(%!data));
}

method from-snapshot(Hash:D $data --> LLM::Data::Pipeline::Context) {
	my LLM::Data::Pipeline::Context $ctx .= new;
	for $data.kv -> Str $k, $v {
		$ctx.set($k, $v);
	}
	$ctx;
}
