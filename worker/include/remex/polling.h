#pragma once
#include "remex/core.h"
#include <grpcpp/support/status.h>

namespace remex {
// Only transient device/control-plane failures are retried. Invalid configuration
// and authentication/authorization failures remain explicit errors.
struct RetryableError : Error {
  using Error::Error;
};
// Idle: the scheduler had no work and answered at once, so the worker pauses
// before it polls again. IdleAfterWait: the scheduler held the call while it
// had no work, so the worker polls again at once.
enum class PollOutcome { Idle, IdleAfterWait, AttemptFinished, RegistrationRequired };
struct PollingHooks {
  std::function<DeviceSnapshot()> probe;
  std::function<void(const DeviceSnapshot&)> register_worker;
  std::function<PollOutcome(const DeviceSnapshot&)> lease;
  std::function<bool()> stopped;
  std::function<void(std::chrono::milliseconds)> wait;
  std::function<void(const std::string&)> log;
};
bool RetryableControlStatus(grpc::StatusCode);
void CheckControlRpc(const grpc::Status&, std::string_view operation);
// This is the production registration/lease loop. Injected hooks let tests
// exercise failures without claiming fabricated NVML snapshots are real devices.
void RunPollingLoop(bool once, const PollingHooks&);
}  // namespace remex
