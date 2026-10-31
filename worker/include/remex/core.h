#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "build/bazel/remote/execution/v2/remote_execution.pb.h"

namespace remex {
namespace fs = std::filesystem;
namespace re = build::bazel::remote::execution::v2;
using Properties = std::map<std::string, std::string>;
struct Error : std::runtime_error { using std::runtime_error::runtime_error; };
std::string ReadFile(const fs::path&, std::uint64_t limit = 1ULL << 30);
void WriteFile(const fs::path&, std::string_view);
re::Digest Hash(std::string_view data);
void Verify(const re::Digest&, std::string_view data);
void ValidateDigest(const re::Digest&);
void ValidateRelative(std::string_view path, bool allow_empty = false);
fs::path SafePath(const fs::path& root, std::string_view relative, bool leaf_symlink = false);
void ValidateSymlink(std::string_view relative_path, std::string_view target);
// Resolve components in filesystem order, expanding links before processing .. .
void ValidateSymlinkAtRoot(const fs::path& root, const fs::path& relative_path);
std::string JsonEscape(std::string_view);
std::string UniqueID();

class BlobStore {
 public:
  virtual ~BlobStore() = default;
  virtual std::string Get(const re::Digest&) = 0;
  virtual void Put(const re::Digest&, std::string_view) = 0;
  re::Digest Upload(std::string_view data);
};
class CachedBlobs : public BlobStore {
 public:
  CachedBlobs(BlobStore& remote, fs::path cache);
  std::string Get(const re::Digest&) override;
  void Put(const re::Digest&, std::string_view) override;
 private:
  BlobStore& remote_;
  fs::path cache_;
};
void Materialize(BlobStore&, const re::Digest&, const fs::path&, const std::atomic<bool>& stop);
re::ActionResult CollectOutputs(BlobStore&, const re::Command&, const fs::path&, int exit_code);

struct ResourceStats {
  std::string cgroup_path;
  std::int64_t oom_kill = 0;
  std::map<std::string,std::uint64_t> cpu;
  std::map<std::string,std::uint64_t> memory;
  std::string JSON() const;
};
class Cgroup {
 public:
  Cgroup(const fs::path& delegated_root, std::string_view attempt);
  ~Cgroup();
  Cgroup(const Cgroup&) = delete;
  Cgroup& operator=(const Cgroup&) = delete;
  void Attach(int pid);
  ResourceStats KillAndWait(std::chrono::milliseconds timeout = std::chrono::seconds(10));
  const fs::path& path() const { return path_; }
  bool empty() const { return empty_; }
 private:
  fs::path path_;
  bool empty_ = false;
  int guardian_write_ = -1;
  int guardian_pid_ = -1;
};
struct ProcessResult {
  int exit_code = -1;
  bool timed_out = false;
  bool cancelled = false;
  ResourceStats resources;
};
ProcessResult RunCommand(const re::Command&, const fs::path& root, const fs::path& log_root,
                         Cgroup&, std::chrono::milliseconds timeout,
                         const std::atomic<bool>& cancelled, const std::function<void()>& started);
struct DeviceSnapshot {
  Properties properties;
  std::string json;
};
DeviceSnapshot ProbeDevices(const Properties& declared);
void MatchPlatform(const re::Platform&, const Properties& actual);
}
