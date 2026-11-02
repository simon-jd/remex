#include "remex/remote.h"
#include <algorithm>
namespace remex {
namespace {
void Deadline(grpc::ClientContext& ctx) {
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
}
void Check(const grpc::Status& status, const char* operation) {
  if (!status.ok())
    throw Error(std::string(operation) + ": " + std::to_string(status.error_code()) + " " +
                status.error_message());
}
}  // namespace
std::shared_ptr<grpc::Channel> Connect(const std::string& endpoint) {
  if (endpoint.empty()) throw Error("empty gRPC endpoint");
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(64 << 20);
  args.SetMaxSendMessageSize(64 << 20);
  // Reconnect within a second of the peer coming back; gRPC's default backoff
  // starts at 1 s and grows 1.6x per failure up to 2 minutes. The minimum
  // connect timeout keeps its 20 s default: lowering it cuts attempts short.
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
  if (endpoint.starts_with("grpcs://"))
    return grpc::CreateCustomChannel(endpoint.substr(8), grpc::SslCredentials({}), args);
  return grpc::CreateCustomChannel(endpoint.starts_with("grpc://") ? endpoint.substr(7) : endpoint,
                                   grpc::InsecureChannelCredentials(), args);
}
RemoteBlobs::RemoteBlobs(const std::string& endpoint, std::string instance,
                         const std::atomic<bool>& stop)
    : stream_(google::bytestream::ByteStream::NewStub(Connect(endpoint))),
      cas_(re::ContentAddressableStorage::NewStub(Connect(endpoint))),
      instance_(std::move(instance)),
      stop_(stop) {}
std::string RemoteBlobs::Get(const re::Digest& digest) {
  ValidateDigest(digest);
  if (stop_) throw Error("lease lost before CAS read");
  if (digest.size_bytes() > 1LL << 30) throw Error("input blob exceeds 1 GiB worker limit");
  if (digest.size_bytes() == 0) {
    Verify(digest, "");
    return "";
  }
  google::bytestream::ReadRequest req;
  req.set_resource_name((instance_.empty() ? "" : instance_ + "/") + "blobs/" + digest.hash() +
                        "/" + std::to_string(digest.size_bytes()));
  grpc::ClientContext context;
  Deadline(context);
  auto reader = stream_->Read(&context, req);
  google::bytestream::ReadResponse response;
  std::string data;
  data.reserve(digest.size_bytes());
  while (reader->Read(&response)) {
    if (stop_ ||
        data.size() + response.data().size() > static_cast<std::uint64_t>(digest.size_bytes())) {
      context.TryCancel();
      break;
    }
    data.append(response.data());
  }
  Check(reader->Finish(), "ByteStream.Read");
  if (stop_) throw Error("lease lost during CAS read");
  Verify(digest, data);
  return data;
}
void RemoteBlobs::Put(const re::Digest& digest, std::string_view data) {
  Verify(digest, data);
  if (stop_) throw Error("lease lost before CAS upload");
  re::FindMissingBlobsRequest find;
  find.set_instance_name(instance_);
  *find.add_blob_digests() = digest;
  re::FindMissingBlobsResponse missing;
  grpc::ClientContext find_context;
  Deadline(find_context);
  Check(cas_->FindMissingBlobs(&find_context, find, &missing), "FindMissingBlobs");
  if (missing.missing_blob_digests().empty()) return;
  const auto resource = (instance_.empty() ? "" : instance_ + "/") + "uploads/" + UniqueID() +
                        "/blobs/" + digest.hash() + "/" + std::to_string(digest.size_bytes());
  grpc::ClientContext context;
  Deadline(context);
  google::bytestream::WriteResponse response;
  auto writer = stream_->Write(&context, &response);
  std::size_t offset = 0;
  do {
    if (stop_) {
      context.TryCancel();
      break;
    }
    const auto size = std::min<std::size_t>(1 << 20, data.size() - offset);
    google::bytestream::WriteRequest request;
    request.set_resource_name(resource);
    request.set_write_offset(offset);
    request.set_data(data.data() + offset, size);
    offset += size;
    request.set_finish_write(offset == data.size());
    if (!writer->Write(request)) break;
  } while (offset < data.size());
  writer->WritesDone();
  Check(writer->Finish(), "ByteStream.Write");
  if (response.committed_size() != digest.size_bytes())
    throw Error("CAS acknowledged wrong committed_size");
  if (stop_) throw Error("lease lost during CAS upload");
}
}  // namespace remex
