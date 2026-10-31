#include "remex/remote.h"
#include <grpcpp/server_builder.h>
#include <iostream>
#include <mutex>
using namespace remex;
class CAS final: public re::ContentAddressableStorage::Service, public google::bytestream::ByteStream::Service {
 public:
  std::map<std::string,std::string> blobs;
  std::string instance="test/instance";
  bool corrupt=false;
  std::mutex mutex;
  grpc::Status FindMissingBlobs(grpc::ServerContext*,const re::FindMissingBlobsRequest* req,re::FindMissingBlobsResponse* resp) override {
    if(req->instance_name()!=instance)return {grpc::StatusCode::INVALID_ARGUMENT,"instance mismatch"};
    std::lock_guard lock(mutex);
    for(const auto& d:req->blob_digests())if(!blobs.contains(d.hash()))*resp->add_missing_blob_digests()=d;
    return grpc::Status::OK;
  }
  grpc::Status Read(grpc::ServerContext*,const google::bytestream::ReadRequest* req,grpc::ServerWriter<google::bytestream::ReadResponse>* writer) override {
    const auto prefix=instance+"/blobs/";if(!req->resource_name().starts_with(prefix))return {grpc::StatusCode::INVALID_ARGUMENT,"instance missing"};
    const auto hash=req->resource_name().substr(prefix.size(),64);std::string data;
    {std::lock_guard lock(mutex);if(!blobs.contains(hash))return {grpc::StatusCode::NOT_FOUND,"missing"};data=blobs[hash];}
    if(corrupt && !data.empty())data[0]^=1;
    for(std::size_t offset=0;offset<data.size();offset+=65536) {google::bytestream::ReadResponse response;response.set_data(data.substr(offset,65536));if(!writer->Write(response))break;}
    return grpc::Status::OK;
  }
  grpc::Status Write(grpc::ServerContext*,grpc::ServerReader<google::bytestream::WriteRequest>* reader,google::bytestream::WriteResponse* response) override {
    google::bytestream::WriteRequest request;std::string data,resource;bool finish=false;
    while(reader->Read(&request)) {
      if(resource.empty())resource=request.resource_name();
      if(resource!=request.resource_name() || request.write_offset()!=static_cast<std::int64_t>(data.size()) || finish)return {grpc::StatusCode::INVALID_ARGUMENT,"bad write sequence"};
      data+=request.data();finish=request.finish_write();
    }
    if(!finish || !resource.starts_with(instance+"/uploads/"))return {grpc::StatusCode::INVALID_ARGUMENT,"incomplete write"};
    auto marker=resource.find("/blobs/");auto slash=resource.find('/',marker+7);
    if(marker==resource.npos || slash==resource.npos)return {grpc::StatusCode::INVALID_ARGUMENT,"invalid resource"};
    re::Digest d;d.set_hash(resource.substr(marker+7,slash-marker-7));d.set_size_bytes(std::stoll(resource.substr(slash+1)));
    try{Verify(d,data);}catch(const std::exception&){return {grpc::StatusCode::INVALID_ARGUMENT,"digest mismatch"};}
    {std::lock_guard lock(mutex);blobs[d.hash()]=data;}response->set_committed_size(data.size());return grpc::Status::OK;
  }
};
int main(){
  CAS cas;int port=0;grpc::ServerBuilder builder;builder.AddListeningPort("127.0.0.1:0",grpc::InsecureServerCredentials(),&port);
  builder.RegisterService(static_cast<re::ContentAddressableStorage::Service*>(&cas));builder.RegisterService(static_cast<google::bytestream::ByteStream::Service*>(&cas));
  auto server=builder.BuildAndStart();if(!server || port==0){std::cerr<<"failed to bind test gRPC server\n";return 1;}
  int result=0;
  try {
    std::atomic<bool> stop=false;RemoteBlobs blobs("127.0.0.1:"+std::to_string(port),cas.instance,stop);
    std::string large(3*1024*1024+17,'x');large.back()='z';auto d=blobs.Upload(large);
    if(blobs.Get(d)!=large)throw Error("ByteStream chunk roundtrip failed");
    blobs.Put(d,large);cas.corrupt=true;bool rejected=false;
    try{blobs.Get(d);}catch(const std::exception&){rejected=true;}if(!rejected)throw Error("corrupted remote CAS was accepted");
    cas.corrupt=false;stop=true;rejected=false;try{blobs.Get(d);}catch(const std::exception&){rejected=true;}if(!rejected)throw Error("read after lease loss accepted");
    std::cout<<"PASS real gRPC ByteStream chunks, instance prefix, deduplicated upload, corruption detection, lease-loss cancellation\n";
  }catch(const std::exception& e){std::cerr<<e.what()<<'\n';result=1;}
  server->Shutdown();return result;
}
