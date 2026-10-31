Vendored, unmodified protobuf schema inputs (Apache-2.0 headers retained):

- `build/bazel/`: [remote-apis v2.12.0](https://github.com/bazelbuild/remote-apis/tree/v2.12.0).
- `google/`: [googleapis commit b6efa15f5537f053786e5d8875d75bc3a6246455](https://github.com/googleapis/googleapis/tree/b6efa15f5537f053786e5d8875d75bc3a6246455).

CMake generates the C++ protobuf and gRPC classes into its build directory. Bazel uses the same schema files and pins gRPC C++ 1.84.0 in the root module.
