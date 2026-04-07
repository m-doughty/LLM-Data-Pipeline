use LLM::Data::Pipeline::Context;

unit role LLM::Data::Pipeline::Step;

method name(--> Str:D) { ... }
method description(--> Str:D) { ... }
method requires(--> List) { () }
method optional(--> List) { () }
method provides(--> List) { ... }
method execute(LLM::Data::Pipeline::Context:D $ctx --> Nil) { ... }
