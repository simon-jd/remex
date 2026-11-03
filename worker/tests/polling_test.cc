#include "remex/polling.h"
#include <iostream>
#include <vector>
using namespace remex;
namespace {
void Require(bool condition, const char* message) {
  if (!condition) throw Error(message);
}
struct Fixture {
  bool stopped = false;
  int probes = 0, registrations = 0, leases = 0;
  std::vector<long long> delays;
  std::vector<std::string> messages, registered, leased;
  PollingHooks hooks;
  Fixture() {
    hooks.probe = [&] {
      DeviceSnapshot result;
      result.json = "probe-" + std::to_string(++probes);
      return result;
    };
    hooks.register_worker = [&](const DeviceSnapshot& device) {
      ++registrations;
      registered.push_back(device.json);
    };
    hooks.lease = [&](const DeviceSnapshot& device) {
      ++leases;
      leased.push_back(device.json);
      stopped = true;
      return PollOutcome::AttemptFinished;
    };
    hooks.stopped = [&] { return stopped; };
    hooks.wait = [&](std::chrono::milliseconds duration) { delays.push_back(duration.count()); };
    hooks.log = [&](const std::string& message) { messages.push_back(message); };
  }
};
void ProbeFailurePreventsRegistrationAndLease() {
  Fixture f;
  f.hooks.probe = [&] {
    ++f.probes;
    if (f.probes <= 6) throw RetryableError("NVML device identity query failed");
    DeviceSnapshot result;
    result.json = "fresh-" + std::to_string(f.probes);
    return result;
  };
  RunPollingLoop(false, f.hooks);
  Require(f.registrations == 1 && f.leases == 1,
          "probe failure must prevent registration and lease RPCs");
  Require(f.registered == std::vector<std::string>{"fresh-7"} &&
              f.leased == std::vector<std::string>{"fresh-8"},
          "a fresh probe must precede every RPC cycle");
  Require(f.delays == std::vector<long long>{500, 1000, 2000, 4000, 5000, 5000},
          "device retry backoff must be bounded");
  Require(f.messages.size() == 6, "each device retry must be logged");
}
void IntermittentProbeCannotReusePriorSnapshot() {
  Fixture f;
  f.hooks.probe = [&] {
    ++f.probes;
    if (f.probes == 3 || f.probes == 4) throw RetryableError("temporary NVML outage");
    DeviceSnapshot result;
    result.json = "current-" + std::to_string(f.probes);
    return result;
  };
  f.hooks.lease = [&](const DeviceSnapshot& device) {
    ++f.leases;
    f.leased.push_back(device.json);
    if (f.leases == 1) return PollOutcome::Idle;
    f.stopped = true;
    return PollOutcome::AttemptFinished;
  };
  RunPollingLoop(false, f.hooks);
  Require(f.leased == std::vector<std::string>{"current-2", "current-5"},
          "lease after outage must never reuse the last successful snapshot");
  Require(f.delays == std::vector<long long>{500, 500, 1000},
          "healthy idle cycle must reset retry backoff");
}
void InitialRegisterRecoversWithFreshProbe() {
  Fixture f;
  f.hooks.register_worker = [&](const DeviceSnapshot& device) {
    ++f.registrations;
    f.registered.push_back(device.json);
    if (f.registrations <= 2)
      CheckControlRpc({grpc::StatusCode::UNAVAILABLE, "scheduler is restarting"}, "Register RPC");
  };
  RunPollingLoop(false, f.hooks);
  Require(f.registrations == 3 && f.leases == 1,
          "worker must wait for registration acknowledgement before leasing");
  Require(f.registered == std::vector<std::string>{"probe-1", "probe-2", "probe-3"} &&
              f.leased == std::vector<std::string>{"probe-4"},
          "registration retries and first lease require fresh identity");
  Require(f.delays == std::vector<long long>{500, 1000}, "registration retries must back off");
}
void SchedulerRestartTriggersReregistration() {
  Fixture f;
  f.hooks.lease = [&](const DeviceSnapshot& device) {
    ++f.leases;
    f.leased.push_back(device.json);
    if (f.leases == 1) return PollOutcome::Idle;
    if (f.leases == 2) return PollOutcome::RegistrationRequired;  // the scheduler restarted
    f.stopped = true;
    return PollOutcome::AttemptFinished;
  };
  RunPollingLoop(false, f.hooks);
  Require(f.registered == std::vector<std::string>{"probe-1", "probe-4"} &&
              f.leased == std::vector<std::string>{"probe-2", "probe-3", "probe-5"},
          "scheduler restart must re-register and re-probe before leasing");
  Require(f.delays == std::vector<long long>{500},
          "a scheduler that asks for registration is up: register again without a backoff");
}
void RepeatedRegistrationRequestsBackOff() {
  Fixture f;
  f.hooks.lease = [&](const DeviceSnapshot& device) {
    ++f.leases;
    f.leased.push_back(device.json);
    if (f.leases <= 4) return PollOutcome::RegistrationRequired;
    f.stopped = true;
    return PollOutcome::AttemptFinished;
  };
  RunPollingLoop(false, f.hooks);
  Require(f.registrations == 5, "every registration request must be honoured");
  Require(f.delays == std::vector<long long>{500, 1000, 1000, 1000},
          "a scheduler that keeps asking right after registration must not be spun on");
}
void SchedulerOutageIsRetriedEverySecond() {
  Fixture f;
  f.hooks.lease = [&](const DeviceSnapshot& device) {
    ++f.leases;
    f.leased.push_back(device.json);
    if (f.leases <= 5)
      CheckControlRpc({grpc::StatusCode::UNAVAILABLE, "connection refused"}, "Lease RPC");
    f.stopped = true;
    return PollOutcome::AttemptFinished;
  };
  RunPollingLoop(false, f.hooks);
  Require(f.leases == 6 && f.registrations == 1,
          "an unreachable scheduler must be retried without re-registering");
  Require(f.delays == std::vector<long long>{500, 1000, 1000, 1000, 1000},
          "scheduler retries must stay at or below 1 s");
}
void LongPollRepeatsWithoutPause() {
  Fixture f;
  f.hooks.lease = [&](const DeviceSnapshot& device) {
    ++f.leases;
    f.leased.push_back(device.json);
    if (f.leases <= 2) return PollOutcome::IdleAfterWait;  // the scheduler held each call
    if (f.leases == 3) return PollOutcome::Idle;           // an older scheduler answered at once
    f.stopped = true;
    return PollOutcome::AttemptFinished;
  };
  RunPollingLoop(false, f.hooks);
  Require(f.leased == std::vector<std::string>{"probe-2", "probe-3", "probe-4", "probe-5"},
          "every lease after a long poll must carry a fresh probe");
  Require(f.delays == std::vector<long long>{500},
          "only a lease the scheduler answered at once may be followed by a pause");
}
void OnceDoesNotHideFailure() {
  for (bool probe_failure : {true, false}) {
    Fixture f;
    if (probe_failure)
      f.hooks.probe = []() -> DeviceSnapshot { throw RetryableError("NVML unavailable"); };
    else
      f.hooks.register_worker = [](const DeviceSnapshot&) {
        CheckControlRpc({grpc::StatusCode::UNAVAILABLE, "offline"}, "Register RPC");
      };
    bool rejected = false;
    try {
      RunPollingLoop(true, f.hooks);
    } catch (const RetryableError&) {
      rejected = true;
    }
    Require(rejected && f.delays.empty() && f.leases == 0,
            "--once must surface probe/registration failure immediately");
  }
}
void PermanentRegistrationFailureAndShutdown() {
  Fixture f;
  f.hooks.register_worker = [](const DeviceSnapshot&) {
    CheckControlRpc({grpc::StatusCode::PERMISSION_DENIED, "identity rejected"}, "Register RPC");
  };
  bool permanent = false;
  try {
    RunPollingLoop(false, f.hooks);
  } catch (const RetryableError&) {
  } catch (const Error&) {
    permanent = true;
  }
  Require(permanent && f.delays.empty() && f.leases == 0,
          "authorization failure must not be hidden by retry loop");
  Fixture stopped;
  stopped.hooks.probe = [&]() -> DeviceSnapshot {
    ++stopped.probes;
    throw RetryableError("NVML offline");
  };
  stopped.hooks.wait = [&](std::chrono::milliseconds) { stopped.stopped = true; };
  RunPollingLoop(false, stopped.hooks);
  Require(stopped.probes == 1 && stopped.registrations == 0 && stopped.leases == 0,
          "shutdown during backoff must not issue another RPC");
}
}  // namespace
int main() {
  try {
    ProbeFailurePreventsRegistrationAndLease();
    IntermittentProbeCannotReusePriorSnapshot();
    InitialRegisterRecoversWithFreshProbe();
    SchedulerRestartTriggersReregistration();
    RepeatedRegistrationRequestsBackOff();
    SchedulerOutageIsRetriedEverySecond();
    LongPollRepeatsWithoutPause();
    OnceDoesNotHideFailure();
    PermanentRegistrationFailureAndShutdown();
    std::cout << "PASS polling recovery, fresh identity gating, bounded backoff, startup/restart "
                 "registration, long polls, fail-fast and shutdown\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
