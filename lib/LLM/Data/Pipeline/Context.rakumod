use JSON::Fast;

unit class LLM::Data::Pipeline::Context;

has %!data;

method set(Str:D $key, $value --> Nil) {
	%!data{$key} = $value;
}

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
