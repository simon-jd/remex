#include "remex/core.h"
#include "remex/remote.h"
#include "remex/polling.h"
#include "api/worker/v1/worker.grpc.pb.h"
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/time_util.h>
#include <algorithm>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace remex {
namespace w = worker::v1;
using Clock = std::chrono::steady_clock;
namespace {
volatile sig_atomic_t shutting_down = 0;
void Signal(int) { shutting_down = 1; }
constexpr std::chrono::milliseconds kLongestLeaseWait(1000);
google::protobuf::Timestamp TimestampNow() {
  const auto elapsed = std::chrono::system_clock::now().time_since_epoch();
  return google::protobuf::util::TimeUtil::NanosecondsToTimestamp(
      std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
}
struct Config {
  std::string scheduler = "localhost:8980", cas, id = UniqueID();
  fs::path root = "/tmp/remex-worker", cgroup_root;
  std::string isolation = "cgroup";
  std::vector<Limit> limits;
  Properties properties{{"pool", "cpu"}};
  bool once = false, probe = false, check = false;
};
Config Parse(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--once") {
      c.once = true;
      continue;
    }
    if (arg == "--probe") {
      c.probe = true;
      continue;
    }
    if (arg == "--check-cgroup") {
      c.check = true;
      continue;
    }
    if (arg == "--help") {
      std::cout << "remex-worker --scheduler HOST:PORT --cgroup-root DELEGATED_PATH [--cas "
                   "HOST:PORT] [--root PATH] [--id ID] [--property KEY=VALUE] [--once]\n"
                   "remex-worker --scheduler HOST:PORT --isolation process-group [--rlimit "
                   "NAME=VALUE]... [same options]\n"
                   "--probe prints host/NVML identity without registering; --check-cgroup verifies "
                   "real cgroup.kill and emptiness.\n"
                   "--isolation process-group is for hosts without a delegated cgroup v2 subtree: "
                   "each command gets its own session and the rlimits, and the worker kills its "
                   "whole process tree; nothing accounts memory or OOM kills.\n";
      std::exit(0);
    }
    if (i + 1 >= argc) throw Error("missing value for " + arg);
    std::string value = argv[++i];
    if (arg == "--scheduler")
      c.scheduler = value;
    else if (arg == "--cas")
      c.cas = value;
    else if (arg == "--root")
      c.root = value;
    else if (arg == "--cgroup-root")
      c.cgroup_root = value;
    else if (arg == "--isolation") {
      if (value != "cgroup" && value != "process-group")
        throw Error("--isolation must be cgroup or process-group");
      c.isolation = value;
    } else if (arg == "--rlimit")
      c.limits.push_back(ParseLimit(value));
    else if (arg == "--id")
      c.id = value;
    else if (arg == "--property") {
      auto n = value.find('=');
      if (n == value.npos || n == 0) throw Error("property must be KEY=VALUE");
      c.properties[value.substr(0, n)] = value.substr(n + 1);
    } else
      throw Error("unknown option " + arg);
  }
  c.root = fs::absolute(c.root);
  if (c.isolation == "process-group" && !c.cgroup_root.empty())
    throw Error("--cgroup-root does not apply to --isolation process-group");
  if (c.isolation == "cgroup" && !c.limits.empty())
    throw Error("--rlimit needs --isolation process-group; a cgroup limits the whole attempt");
  return c;
}
// Every RPC to the scheduler waits, until its deadline, for the channel to
// connect. A fail-fast RPC on a channel in TRANSIENT_FAILURE returns at once,
// and with no RPC waiting on the connection gRPC only advances a background
// reconnect from its 5 s backup poller: a restarted scheduler stayed
// unreachable for seconds while the lease ran out.
void ControlRpc(grpc::ClientContext& ctx,
                std::chrono::milliseconds duration = std::chrono::seconds(5)) {
  ctx.set_deadline(std::chrono::system_clock::now() + duration);
  ctx.set_wait_for_ready(true);
}
class LeaseGuard {
 public:
  LeaseGuard(w::WorkerService::Stub& stub, const Config& config, const w::LeaseResponse& lease,
             Clock::time_point lease_start, std::int64_t interval)
      : stub_(stub),
        worker_(config.id),
        attempt_(lease.attempt_id()),
        epoch_(lease.epoch()),
        expiry_(lease_start + std::chrono::milliseconds(lease.lease_duration_ms())),
        interval_(std::clamp<std::int64_t>(
            interval, 50, std::max<std::int64_t>(50, lease.lease_duration_ms() / 3))) {
    if (lease.lease_duration_ms() < 150) throw Error("lease duration must be at least 150 ms");
    thread_ = std::thread([this] {
      auto wait = std::chrono::milliseconds(interval_);
      while (true) {
        {
          std::unique_lock lock(wait_mutex_);
          if (cv_.wait_for(lock, wait, [this] { return done_; })) break;
        }
        if (shutting_down) {
          lost = true;
          break;
        }
        const auto result = Try();
        if (result == BeatResult::kLost) break;
        // The lease keeps running while the scheduler is unreachable, so a
        // failed heartbeat is retried soon rather than a whole interval later:
        // a scheduler back within the lease must hear from the attempt first.
        wait = std::chrono::milliseconds(
            result == BeatResult::kRetry ? std::min<std::int64_t>(interval_, 100) : interval_);
      }
    });
  }
  ~LeaseGuard() {
    {
      std::lock_guard lock(wait_mutex_);
      done_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
  }
  // A phase change must be acknowledged before the attempt moves on, so it
  // retries transport failures until the scheduler answers or the lease ends.
  void Phase(w::HeartbeatRequest::Phase value) {
    phase_ = value;
    for (BeatResult result; (result = Try()) != BeatResult::kOk;) {
      if (result == BeatResult::kLost) throw Error("lease lost on phase transition");
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  // False only once the lease is lost. A heartbeat that fails in transport
  // (timeout, scheduler unavailable) is retried at the next interval: the
  // attempt is abandoned when the scheduler says so (FENCED / CANCEL) or when
  // the local lease, measured from the start of the last acknowledged
  // heartbeat, runs out. Giving up on the first slow RPC turned any
  // scheduler stall over 1 s into lost work on every worker.
  bool Beat() { return Try() != BeatResult::kLost; }
  // True once the local lease has run out; heartbeats extend it while the
  // scheduler answers.
  bool Expired() {
    std::lock_guard lock(rpc_mutex_);
    return Clock::now() >= expiry_;
  }
  std::atomic<bool> lost{false};

 private:
  enum class BeatResult { kOk, kRetry, kLost };
  BeatResult Try() {
    std::lock_guard lock(rpc_mutex_);
    const auto start = Clock::now();
    if (lost || start >= expiry_ || shutting_down) {
      lost = true;
      return BeatResult::kLost;
    }
    w::HeartbeatRequest request;
    request.set_worker_id(worker_);
    request.set_attempt_id(attempt_);
    request.set_epoch(epoch_);
    request.set_phase(phase_);
    w::HeartbeatResponse response;
    grpc::ClientContext context;
    ControlRpc(context,
               std::min(std::chrono::milliseconds(1000),
                        std::chrono::duration_cast<std::chrono::milliseconds>(expiry_ - start)));
    auto status = stub_.Heartbeat(&context, request, &response);
    if (!status.ok()) {
      if (Clock::now() >= expiry_) {
        lost = true;
        return BeatResult::kLost;
      }
      return BeatResult::kRetry;
    }
    if (response.decision() != w::HeartbeatResponse::CONTINUE ||
        response.lease_duration_ms() <= 0 || Clock::now() >= expiry_) {
      lost = true;
      return BeatResult::kLost;
    }
    expiry_ = start + std::chrono::milliseconds(response.lease_duration_ms());
    if (Clock::now() >= expiry_) {
      lost = true;
      return BeatResult::kLost;
    }
    return BeatResult::kOk;
  }
  w::WorkerService::Stub& stub_;
  std::string worker_, attempt_;
  std::int64_t epoch_;
  Clock::time_point expiry_;
  std::int64_t interval_;
  std::atomic<w::HeartbeatRequest::Phase> phase_{w::HeartbeatRequest::MATERIALIZING};
  std::mutex rpc_mutex_, wait_mutex_;
  std::condition_variable cv_;
  bool done_ = false;
  std::thread thread_;
};
void Attempt(w::WorkerService::Stub& stub, const Config& config, const w::LeaseResponse& lease,
             const DeviceSnapshot& device, Clock::time_point lease_start, std::int64_t interval) {
  re::ExecutedActionMetadata metadata;
  metadata.set_worker(config.id);
  if (lease.has_queued_timestamp()) {
    *metadata.mutable_queued_timestamp() = lease.queued_timestamp();
  }
  *metadata.mutable_worker_start_timestamp() = TimestampNow();
  LeaseGuard guard(stub, config, lease, lease_start, interval);
  const auto directory = config.root / "attempts" / UniqueID();
  if (!fs::create_directory(directory)) throw Error("attempt directory collision");
  const auto input = directory / "root", logs = directory / "logs";
  fs::create_directory(input);
  // Never report cgroup_empty for an attempt whose cgroup or session could not be set up.
  std::unique_ptr<Isolation> isolation;
  if (config.isolation == "process-group")
    isolation = std::make_unique<ProcessGroup>(config.limits);
  else
    isolation = std::make_unique<Cgroup>(config.cgroup_root, directory.filename().string());
  w::CompleteRequest complete;
  complete.set_worker_id(config.id);
  complete.set_attempt_id(lease.attempt_id());
  complete.set_epoch(lease.epoch());
  complete.set_attempt_dir(directory.string());
  complete.set_device_snapshot_json(device.json);
  re::ActionResult result;
  int status_code = 0;
  std::string status_message, failure = "SUCCESS";
  ResourceStats resources;
  try {
    if (!guard.Beat()) throw Error("lease expired before materialization");
    re::Digest action_digest;
    action_digest.set_hash(lease.action_digest().hash());
    action_digest.set_size_bytes(lease.action_digest().size_bytes());
    Verify(action_digest, lease.action());
    re::Action action;
    if (!action.ParseFromString(lease.action())) throw Error("invalid Action proto");
    Verify(action.command_digest(), lease.command());
    re::Command command;
    if (!command.ParseFromString(lease.command())) throw Error("invalid Command proto");
    MatchPlatform(action.platform(), device.properties);
    if (config.properties.at("pool") == "gpu") {
      auto checked = ProbeDevices(config.properties);
      if (checked.json != device.json) throw Error("GPU identity changed after lease");
    }
    RemoteBlobs remote(config.cas.empty() ? lease.cas_endpoint() : config.cas,
                       lease.instance_name(), guard.lost);
    CachedBlobs blobs(remote, config.root / "cache");
    *metadata.mutable_input_fetch_start_timestamp() = TimestampNow();
    Materialize(blobs, action.input_root_digest(), input, guard.lost);
    *metadata.mutable_input_fetch_completed_timestamp() = TimestampNow();
    // REAPI requires parent directories of outputs to exist before starting the command.
    ValidateRelative(command.working_directory(), true);
    const auto cwd = SafePath(input, command.working_directory());
    std::vector<std::string> outputs;
    for (const auto& p : command.output_paths()) outputs.push_back(p);
    for (const auto& p : command.output_files()) outputs.push_back(p);
    for (const auto& p : command.output_directories()) outputs.push_back(p);
    for (const auto& p : outputs) {
      ValidateRelative(p, true);
      auto out = SafePath(cwd, p, true);
      fs::create_directories(out.parent_path());
    }
    auto timeout = std::chrono::milliseconds(0);
    if (action.has_timeout()) {
      const auto& limit = action.timeout();
      if (limit.seconds() < 0 || limit.nanos() < 0 || limit.nanos() >= 1000000000 ||
          limit.seconds() > 86400 * 30LL)
        throw Error("invalid or over-30-day action timeout");
      timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::seconds(action.timeout().seconds()) +
          std::chrono::nanoseconds(action.timeout().nanos()));
      if (timeout.count() == 0) timeout = std::chrono::milliseconds(1);
    }
    auto process = RunCommand(command, input, logs, *isolation, timeout, guard.lost, [&] {
      guard.Phase(w::HeartbeatRequest::RUNNING);
      *metadata.mutable_execution_start_timestamp() = TimestampNow();
    });
    *metadata.mutable_execution_completed_timestamp() = TimestampNow();
    resources = process.resources;
    if (process.cancelled || guard.lost) throw Error("lease lost during execution");
    if (process.timed_out) {
      status_code = 4;
      failure = "TIMEOUT";
      status_message = "action exceeded command timeout";
    } else if (resources.oom_kill > 0) {
      status_code = 8;
      failure = "INFRASTRUCTURE";
      status_message = "cgroup recorded OOM kill";
    } else if (process.exit_code != 0)
      failure = "TEST_FAILURE";
    guard.Phase(w::HeartbeatRequest::UPLOADING);
    *metadata.mutable_output_upload_start_timestamp() = TimestampNow();
    result = CollectOutputs(blobs, command, input, process.exit_code);
    *result.mutable_stdout_digest() = blobs.Upload(ReadFile(logs / "stdout"));
    *result.mutable_stderr_digest() = blobs.Upload(ReadFile(logs / "stderr"));
    *metadata.mutable_output_upload_completed_timestamp() = TimestampNow();
    if (device.json != "{}") {
      google::protobuf::Struct snapshot;
      auto parsed = google::protobuf::util::JsonStringToMessage(device.json, &snapshot);
      if (!parsed.ok()) throw Error("cannot encode device snapshot");
      metadata.add_auxiliary_metadata()->PackFrom(snapshot);
    }
  } catch (const std::exception& error) {
    status_code = 13;
    status_message = error.what();
    failure = "INFRASTRUCTURE";
    std::cerr << "attempt " << lease.attempt_id() << ": " << error.what() << '\n';
  }
  // Cleanup failure is fatal to publication. Destructor retries, but no Complete is sent.
  resources = isolation->KillAndWait();
  WriteFile(directory / "resources.json", resources.JSON());
  WriteFile(directory / "device.json", device.json);
  if (guard.lost || shutting_down) {
    std::cerr << "lease lost; attempt abandoned after cgroup cleanup\n";
    return;
  }
  complete.set_cgroup_empty(isolation->empty());
  complete.set_oom_kill(resources.oom_kill);
  complete.set_resource_usage_json(resources.JSON());
  *metadata.mutable_worker_completed_timestamp() = TimestampNow();
  *result.mutable_execution_metadata() = metadata;
  complete.set_status_code(status_code);
  complete.set_status_message(status_message);
  complete.set_failure_category(failure);
  complete.set_action_result(result.SerializeAsString());
  // Complete is idempotent: replaying the committed winner is ACCEPTED, and a
  // fenced attempt is FENCED again. So a transport failure is retried until
  // the scheduler answers or the local lease runs out; three quick tries used
  // to throw away a finished action whenever the scheduler restarted under it.
  // A try that is waiting for the channel may outlast the lease. Nothing runs
  // any more, and the scheduler's fencing decides whether the result counts.
  while (true) {
    w::CompleteResponse response;
    grpc::ClientContext context;
    ControlRpc(context);
    auto status = stub.Complete(&context, complete, &response);
    if (status.ok()) {
      std::cout << "attempt=" << lease.attempt_id() << " epoch=" << lease.epoch()
                << " decision=" << response.decision() << " " << response.message() << std::endl;
      return;
    }
    if (!RetryableControlStatus(status.error_code()) || guard.Expired() || shutting_down) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw Error("Complete not acknowledged; scheduler fencing controls retry");
}
}  // namespace
int Main(int argc, char** argv) {
  const auto config = Parse(argc, argv);
  if (config.probe) {
    const auto device = ProbeDevices(config.properties);
    std::cout << "{\"properties\":{";
    bool first = true;
    for (const auto& [k, v] : device.properties) {
      if (!first) std::cout << ',';
      first = false;
      std::cout << JsonEscape(k) << ':' << JsonEscape(v);
    }
    std::cout << "},\"device\":" << device.json << "}\n";
    return 0;
  }
  if (config.isolation == "process-group") {
    ProcessGroup preflight(config.limits);
    preflight.KillAndWait();
    if (config.check) {
      std::cout << "process-group subreaper and empty verification passed\n";
      return 0;
    }
  } else {
    if (config.cgroup_root.empty())
      throw Error("--cgroup-root requires an explicitly delegated cgroup v2 subtree");
    {
      Cgroup preflight(config.cgroup_root, "remex-check-" + UniqueID());
      preflight.KillAndWait();
    }
    if (config.check) {
      std::cout << "cgroup v2 kill and empty verification passed\n";
      return 0;
    }
  }
  fs::create_directories(config.root / "attempts");
  fs::create_directories(config.root / "cache");
  auto stub = w::WorkerService::NewStub(Connect(config.scheduler));
  w::RegisterResponse registered;
  PollingHooks hooks;
  hooks.probe = [&] { return ProbeDevices(config.properties); };
  hooks.register_worker = [&](const DeviceSnapshot& device) {
    w::RegisterRequest request;
    request.set_worker_id(config.id);
    for (const auto& [key, value] : device.properties) (*request.mutable_properties())[key] = value;
    request.set_device_snapshot_json(device.json);
    grpc::ClientContext context;
    ControlRpc(context);
    CheckControlRpc(stub->Register(&context, request, &registered), "Register RPC");
    std::cout << "registered worker=" << config.id << " pool=" << config.properties.at("pool")
              << " isolation=" << config.isolation << std::endl;
  };
  hooks.lease = [&](const DeviceSnapshot& device) {
    w::LeaseRequest request;
    request.set_worker_id(config.id);
    request.set_device_snapshot_json(device.json);
    for (const auto& [key, value] : device.properties) (*request.mutable_properties())[key] = value;
    // A scheduler that can hold an idle Lease answers as soon as work is
    // queued for this worker. The worker asks for at most a second, so a
    // stop request waiting behind the call is still seen within one.
    const auto wait = std::chrono::milliseconds(
        std::min<std::int64_t>(registered.lease_wait_ms(), kLongestLeaseWait.count()));
    request.set_wait_ms(wait.count());
    w::LeaseResponse lease;
    grpc::ClientContext context;
    ControlRpc(context, std::chrono::seconds(5) + wait);
    const auto lease_start = Clock::now();
    const auto status = stub->Lease(&context, request, &lease);
    if (status.error_code() == grpc::StatusCode::FAILED_PRECONDITION)
      return PollOutcome::RegistrationRequired;
    CheckControlRpc(status, "Lease RPC");
    if (!lease.available())
      return wait.count() > 0 ? PollOutcome::IdleAfterWait : PollOutcome::Idle;
    try {
      Attempt(*stub, config, lease, device, lease_start, registered.heartbeat_interval_ms());
    } catch (const std::exception& error) {
      if (config.once) throw;
      std::cerr << error.what() << '\n';
    }
    return PollOutcome::AttemptFinished;
  };
  hooks.stopped = [] { return shutting_down != 0; };
  hooks.wait = [](std::chrono::milliseconds duration) {
    const auto deadline = Clock::now() + duration;
    while (!shutting_down && Clock::now() < deadline)
      std::this_thread::sleep_for(
          std::min(std::chrono::milliseconds(100),
                   std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now())));
  };
  hooks.log = [](const std::string& message) { std::cerr << message << '\n'; };
  RunPollingLoop(config.once, hooks);
  return 0;
}
}  // namespace remex
int main(int argc, char** argv) {
  std::signal(SIGINT, remex::Signal);
  std::signal(SIGTERM, remex::Signal);
  std::signal(SIGPIPE, SIG_IGN);
  try {
    return remex::Main(argc, argv);
  } catch (const std::exception& error) {
    std::cerr << "remex-worker: " << error.what() << '\n';
    return 1;
  }
}
