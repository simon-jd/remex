#include "remex/core.h"
#include "remex/polling.h"
#include <cstdlib>
#include <dlfcn.h>
#include <iostream>

using namespace remex;
namespace {
std::size_t Descriptors() {
  return std::distance(fs::directory_iterator("/proc/self/fd"), fs::directory_iterator{});
}
}  // namespace
int main() {
  if (!std::getenv("REMEX_TEST_NVML")) {
    std::cout << "SKIP: set REMEX_TEST_NVML=1 with one real visible GPU and matching NVML\n";
    return 77;
  }
  try {
    const Properties properties{{"pool", "gpu"}};
    const auto first = ProbeDevices(properties);
    const auto baseline = Descriptors();
    for (int i = 1; i <= 600; ++i) {
      const auto current = ProbeDevices(properties);
      if (current.json != first.json || current.properties != first.properties)
        throw Error("device identity changed during repeated real NVML queries");
      if (i % 50 == 0) {
        const auto current_fds = Descriptors();
        std::cout << "NVML queries=" << i << " baseline_fds=" << baseline
                  << " current_fds=" << current_fds << '\n';
        if (current_fds > baseline + 4)
          throw Error("repeated NVML queries leaked file descriptors");
      }
    }
    std::cout << "PASS 600 fresh real NVML identity queries keep descriptors bounded " << first.json
              << '\n';
    // Shut down only this test process's NVML client session. The next real
    // query must fail closed, then a subsequent probe must reinitialize and
    // query successfully. No driver, device, or system service is modified.
    void* library = dlopen("libnvidia-ml.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!library) throw Error("cannot retain NVML for failure/reinitialize test");
    auto shutdown = reinterpret_cast<int (*)()>(dlsym(library, "nvmlShutdown"));
    if (!shutdown) {
      dlclose(library);
      throw Error("NVML shutdown symbol missing");
    }
    for (int i = 1; i <= 100; ++i) {
      if (shutdown() != 0) throw Error("test could not close its NVML client session");
      bool rejected = false;
      try {
        ProbeDevices(properties);
      } catch (const RetryableError&) {
        rejected = true;
      }
      if (!rejected) throw Error("invalid NVML session did not fail closed");
      if (ProbeDevices(properties).json != first.json)
        throw Error("NVML reinitialization lost identity");
      if (Descriptors() > baseline + 4)
        throw Error("NVML failure/reinitialization leaked descriptors");
    }
    dlclose(library);
    std::cout << "PASS 100 real NVML client-session failures/reinitializations keep descriptors "
                 "bounded\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL " << error.what() << '\n';
    return 1;
  }
}
