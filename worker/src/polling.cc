#include "remex/polling.h"
#include <algorithm>

namespace remex {
bool RetryableControlStatus(grpc::StatusCode code) {
  switch (code) {
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::DEADLINE_EXCEEDED:
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
    case grpc::StatusCode::ABORTED:
    case grpc::StatusCode::CANCELLED:
    case grpc::StatusCode::INTERNAL:
      return true;
    default:
      return false;
  }
}
void CheckControlRpc(const grpc::Status& status, std::string_view operation) {
  if (status.ok()) return;
  const auto message = std::string(operation) + ": " + std::to_string(status.error_code()) + " " + status.error_message();
  if (RetryableControlStatus(status.error_code())) throw RetryableError(message);
  throw Error(message);
}
void RunPollingLoop(bool once, const PollingHooks& hooks) {
  bool registered = false, just_registered = false;
  auto backoff = std::chrono::milliseconds(500);
  // A failing device probe backs off to 5 s. The scheduler is retried at
  // least every second: a restarted scheduler should see its workers again
  // within a lease, not after a backoff that grew while it was down.
  const auto maximum = std::chrono::milliseconds(5000), scheduler_maximum = std::chrono::milliseconds(1000);
  while (!hooks.stopped()) {
    bool probing = true;
    try {
      // This snapshot lives for one RPC cycle only. A failed probe cannot reuse
      // a previous cycle's device identity for registration or lease acquisition.
      const auto device = hooks.probe();
      probing = false;
      if (hooks.stopped()) break;
      if (!registered) {
        hooks.register_worker(device);
        registered = just_registered = true;
        continue; // Probe again immediately before the first Lease RPC.
      }
      const auto outcome = hooks.lease(device);
      if (outcome == PollOutcome::RegistrationRequired) {
        registered = false;
        // A scheduler that answers is up; a restarted one has forgotten this
        // worker, so register again at once. Back off only if it asks again
        // straight after a registration.
        if (!just_registered) {
          hooks.log("scheduler requested worker registration");
          continue;
        }
        hooks.log("scheduler requested registration again; retrying in " + std::to_string(backoff.count()) + " ms");
        hooks.wait(backoff);
        backoff = std::min(backoff * 2, scheduler_maximum);
        continue;
      }
      just_registered = false;
      backoff = std::chrono::milliseconds(500);
      if (once && outcome == PollOutcome::AttemptFinished) return;
      if (outcome == PollOutcome::Idle) hooks.wait(std::chrono::milliseconds(500));
    } catch (const RetryableError& error) {
      if (once) throw;
      const auto limit = probing ? maximum : scheduler_maximum;
      backoff = std::min(backoff, limit);
      hooks.log(std::string(error.what()) + "; lease acquisition paused, retrying in " + std::to_string(backoff.count()) + " ms");
      hooks.wait(backoff);
      backoff = std::min(backoff * 2, limit);
    }
  }
}
}
