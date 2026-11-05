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
struct Error : std::runtime_error {
  using std::runtime_error::runtime_error;
};
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
  std::string isolation;  // empty for a cgroup
  std::string cgroup_path;
  std::int64_t oom_kill = 0;
  std::map<std::string, std::uint64_t> cpu;
  std::map<std::string, std::uint64_t> memory;
  std::string JSON() const;
};
// A resource limit the command's process sets before exec (setrlimit), soft and hard alike.
struct Limit {
  int resource = 0;
  std::uint64_t value = 0;
};
// NAME=VALUE with NAME one of as, core, cpu, data, fsize, nofile, nproc, stack and VALUE a
// number or "unlimited".
Limit ParseLimit(std::string_view);
// What an attempt's processes run in. KillAndWait kills all of them and returns once none is
// left; only then is empty() true, and only then may the worker report the attempt.
class Isolation {
 public:
  virtual ~Isolation() = default;
  virtual void Attach(int pid) = 0;
  virtual ResourceStats KillAndWait(
      std::chrono::milliseconds timeout = std::chrono::seconds(10)) = 0;
  virtual bool empty() const = 0;
  // Whether the command starts a session of its own, and the limits it sets, before exec.
  virtual bool new_session() const { return false; }
  virtual const std::vector<Limit>& limits() const;
};
class Cgroup : public Isolation {
 public:
  Cgroup(const fs::path& delegated_root, std::string_view attempt);
  ~Cgroup() override;
  Cgroup(const Cgroup&) = delete;
  Cgroup& operator=(const Cgroup&) = delete;
  void Attach(int pid) override;
  ResourceStats KillAndWait(std::chrono::milliseconds timeout = std::chrono::seconds(10)) override;
  const fs::path& path() const { return path_; }
  bool empty() const override { return empty_; }

 private:
  fs::path path_;
  bool empty_ = false;
  int guardian_write_ = -1;
  int guardian_pid_ = -1;
};
// For hosts without a delegated cgroup v2 subtree. The command starts a session (and process
// group) of its own with the given rlimits; the worker becomes a child subreaper, so whatever
// leaves that group is re-parented to the worker. KillAndWait kills the group and every child
// of the worker until none is left. A guardian process kills the group if the worker dies.
// Nothing accounts memory or counts OOM kills, rlimits apply per process rather than to the
// whole attempt, and a process that leaves the session after the worker dies survives it.
// Needs a worker with no other child processes, running one attempt at a time.
class ProcessGroup : public Isolation {
 public:
  explicit ProcessGroup(std::vector<Limit> limits = {});
  ~ProcessGroup() override;
  ProcessGroup(const ProcessGroup&) = delete;
  ProcessGroup& operator=(const ProcessGroup&) = delete;
  void Attach(int pid) override;
  ResourceStats KillAndWait(std::chrono::milliseconds timeout = std::chrono::seconds(10)) override;
  bool empty() const override { return empty_; }
  bool new_session() const override { return true; }
  const std::vector<Limit>& limits() const override { return limits_; }

 private:
  void Disarm();
  std::vector<Limit> limits_;
  int leader_ = -1;
  bool empty_ = false;
  int guardian_write_ = -1;
  int guardian_pid_ = -1;
  std::uint64_t user_usec_ = 0, system_usec_ = 0;  // children's CPU time before this attempt
};
struct ProcessResult {
  int exit_code = -1;
  bool timed_out = false;
  bool cancelled = false;
  ResourceStats resources;
};
ProcessResult RunCommand(const re::Command&, const fs::path& root, const fs::path& log_root,
                         Isolation&, std::chrono::milliseconds timeout,
                         const std::atomic<bool>& cancelled, const std::function<void()>& started);
struct DeviceSnapshot {
  Properties properties;
  std::string json;
};
DeviceSnapshot ProbeDevices(const Properties& declared);
void MatchPlatform(const re::Platform&, const Properties& actual);
}  // namespace remex
