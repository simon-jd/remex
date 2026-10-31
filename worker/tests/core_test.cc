#include "remex/core.h"
#include <iostream>
#include <set>
#include <unistd.h>
using namespace remex;
namespace {
int failures=0;
void Check(bool result,const char* message){if(!result){std::cerr<<"FAIL "<<message<<'\n';++failures;}}
template<class F>void Reject(F fn,const char* message){try{fn();Check(false,message);}catch(const std::exception&){}}
struct MemoryBlobs:BlobStore{std::map<std::string,std::string> values;int reads=0;
 std::string Get(const re::Digest& d) override {++reads;return values.at(d.hash());}
 void Put(const re::Digest& d,std::string_view data) override {Verify(d,data);values[d.hash()]=data;}
};
}
int main(){
 const auto temp=fs::temp_directory_path()/ ("remex-worker-test-"+UniqueID());fs::create_directory(temp);
 Check(Hash("abc").hash()=="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","SHA256 standard vector");
 auto digest=Hash("abc");Reject([&]{Verify(digest,"abd");},"digest corruption rejected");digest.set_size_bytes(2);Reject([&]{Verify(digest,"abc");},"digest size mismatch rejected");
 for(auto p:{"../escape","/absolute","a/../x","a//b",".","a/"})Reject([&]{ValidateRelative(p);},"path traversal rejected");
 ValidateRelative("a/b-c");ValidateSymlink("dir/link","../file");Reject([]{ValidateSymlink("link","../escape");},"symlink escape rejected");
 fs::create_symlink("/tmp",temp/"outside");Reject([&]{SafePath(temp,"outside/file");},"symlink parent traversal rejected");
 MemoryBlobs memory;auto content=memory.Upload("hello world");re::Directory sub;auto* f=sub.add_files();f->set_name("hello");*f->mutable_digest()=content;f->set_is_executable(true);
 auto subdigest=memory.Upload(sub.SerializeAsString());re::Directory root;auto* child=root.add_directories();child->set_name("nested");*child->mutable_digest()=subdigest;
 auto rootdigest=memory.Upload(root.SerializeAsString());auto input=temp/"input";fs::create_directory(input);std::atomic<bool> stop=false;
 Materialize(memory,rootdigest,input,stop);Check(ReadFile(input/"nested/hello")=="hello world","input tree materialized");Check((fs::status(input/"nested/hello").permissions()&fs::perms::owner_exec)!=fs::perms::none,"executable preserved");
 Reject([&]{Materialize(memory,rootdigest,input,stop);},"attempt input directory never reused");
 auto malicious=root;malicious.mutable_directories(0)->set_name("..");auto malicious_digest=memory.Upload(malicious.SerializeAsString());auto bad=temp/"bad";fs::create_directory(bad);Reject([&]{Materialize(memory,malicious_digest,bad,stop);},"untrusted Directory traversal rejected");
 auto duplicated=sub;*duplicated.add_files()=sub.files(0);auto dupdigest=memory.Upload(duplicated.SerializeAsString());auto dup=temp/"dup";fs::create_directory(dup);Reject([&]{Materialize(memory,dupdigest,dup,stop);},"duplicate node rejected");
 CachedBlobs cache(memory,temp/"cache");int prior=memory.reads;Check(cache.Get(content)=="hello world","cache fills");Check(cache.Get(content)=="hello world" && memory.reads==prior+1,"cache hit avoids download");WriteFile(temp/"cache"/content.hash(),"corrupt");Check(cache.Get(content)=="hello world" && memory.reads==prior+2,"corrupted cache refetched");
 re::Command command;command.add_output_paths("nested");auto result=CollectOutputs(memory,command,input,7);Check(result.exit_code()==7 && result.output_directories_size()==1,"output tree collected");re::Tree tree;Check(tree.ParseFromString(memory.Get(result.output_directories(0).tree_digest())) && tree.root().files_size()==1,"Tree wire encoding");
 re::Command working;working.set_working_directory("nested");working.add_output_paths("hello");auto from_working=CollectOutputs(memory,working,input,0);Check(from_working.output_files_size()==1 && from_working.output_files(0).path()=="hello" && from_working.output_files(0).digest().hash()==content.hash(),"outputs relative to command working directory");
 re::Command whole;whole.add_output_directories("");auto all=CollectOutputs(memory,whole,input,0);Check(all.output_directories_size()==1 && all.output_directories(0).path().empty(),"empty output directory captures whole working directory");
 fs::create_symlink("../../etc/passwd",input/"escape");re::Command escape;escape.add_output_paths("escape");Reject([&]{CollectOutputs(memory,escape,input,0);},"output symlink escape rejected");
 // A lexical a/../path check is insufficient when a itself resolves to '.'.
 auto chain_root=temp/"chain";fs::create_directory(chain_root);WriteFile(temp/"sibling-secret","outside");
 fs::create_symlink(".",chain_root/"a");fs::create_symlink("a/../sibling-secret",chain_root/"b");
 Reject([&]{ValidateSymlinkAtRoot(chain_root,"b");},"symlink chain cannot traverse above actual root");
 re::Command chained_output;chained_output.add_output_paths("b");Reject([&]{CollectOutputs(memory,chained_output,chain_root,0);},"output symlink graph escape rejected");
 re::Directory chained_input;auto* ca=chained_input.add_symlinks();ca->set_name("a");ca->set_target(".");auto* cb=chained_input.add_symlinks();cb->set_name("b");cb->set_target("a/../sibling-secret");
 auto chain_digest=memory.Upload(chained_input.SerializeAsString());auto chain_input=temp/"chain-input";fs::create_directory(chain_input);
 Reject([&]{Materialize(memory,chain_digest,chain_input,stop);},"input symlink graph escape rejected before execution");
 fs::create_directory(chain_root/"runfiles");fs::create_symlink("../a/target",chain_root/"runfiles"/"valid");ValidateSymlinkAtRoot(chain_root,"runfiles/valid");
 fs::create_symlink("cycle2",chain_root/"cycle1");fs::create_symlink("cycle1",chain_root/"cycle2");Reject([&]{ValidateSymlinkAtRoot(chain_root,"cycle1");},"cyclic symlink graph rejected");
 re::Platform platform;auto* prop=platform.add_properties();prop->set_name("cpu.min");prop->set_value("2");MatchPlatform(platform,{{"cpu.min","4"}});Reject([&]{MatchPlatform(platform,{{"cpu.min","1"}});},"resource lower bound enforced");prop->set_value("nan");Reject([&]{MatchPlatform(platform,{{"cpu.min","4"}});},"non-finite resource rejected");
 re::Platform hardware;auto* marker=hardware.add_properties();marker->set_name("remex-hw");marker->set_value("true");MatchPlatform(hardware,{});
 Reject([&]{Cgroup fake(temp,"fake");},"ordinary directories cannot fake cgroup proof");
 stop=true;auto stopped=temp/"stopped";fs::create_directory(stopped);Reject([&]{Materialize(memory,rootdigest,stopped,stop);},"lost lease cancels materialization");
 fs::remove_all(temp);std::cout<<(failures?"FAILED":"PASS")<<" worker core checks\n";return failures?1:0;
}
