#pragma once
#include "remex/core.h"
#include <grpcpp/grpcpp.h>
#include "google/bytestream/bytestream.grpc.pb.h"
#include "build/bazel/remote/execution/v2/remote_execution.grpc.pb.h"
namespace remex {
std::shared_ptr<grpc::Channel> Connect(const std::string& endpoint);
class RemoteBlobs final : public BlobStore {
 public:
  RemoteBlobs(const std::string& endpoint, std::string instance, const std::atomic<bool>& stop);
  std::string Get(const re::Digest&) override;
  void Put(const re::Digest&, std::string_view) override;

 private:
  std::unique_ptr<google::bytestream::ByteStream::Stub> stream_;
  std::unique_ptr<re::ContentAddressableStorage::Stub> cas_;
  std::string instance_;
  const std::atomic<bool>& stop_;
};
}  // namespace remex
