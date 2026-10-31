The `working_directory` fixture contains official REAPI v2 `Tree` and
`ActionResult` protobuf wire messages, plus their human-readable textproto and
the referenced output file content. `OutputDirectory.path` is the empty string
(the proto3 default, omitted on the wire), which denotes the entire command
working directory. The file digest and Tree digest are SHA-256 of the exact
stored payload and Tree bytes respectively.
