#include "remex/core.h"
#include "remex/polling.h"
#include <dlfcn.h>
#include <cmath>
#include <mutex>
#include <sys/sysinfo.h>
#include <thread>

namespace remex {
namespace {
struct Library { void* handle = nullptr; ~Library() { if (handle) dlclose(handle); } };
template<class T> T Symbol(void* lib, const char* name) {
  auto* address = dlsym(lib, name); if (!address) throw Error(std::string("NVML symbol missing: ") + name); return reinterpret_cast<T>(address);
}
using Device = void*;
// NVML is a process service: repeated dlopen/init/shutdown/dlclose cycles in
// NVIDIA 595.91 leak an eventfd per cycle. Keep the library and initialized
// session alive, but query the device identity afresh on every call. A failed
// query invalidates the session; the next probe must initialize again.
struct Nvml {
  Library library;
  std::mutex mutex;
  int (*init)();
  int (*shutdown)();
  int (*count)(unsigned*);
  int (*device)(unsigned, Device*);
  int (*uuid)(Device, char*, unsigned);
  int (*capability)(Device, int*, int*);
  int (*driver)(char*, unsigned);
  bool initialized = false;
  Nvml() {
    library.handle = dlopen("libnvidia-ml.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!library.handle) throw RetryableError("GPU pool requires loadable NVML");
    init = Symbol<decltype(init)>(library.handle, "nvmlInit_v2");
    shutdown = Symbol<decltype(shutdown)>(library.handle, "nvmlShutdown");
    count = Symbol<decltype(count)>(library.handle, "nvmlDeviceGetCount_v2");
    device = Symbol<decltype(device)>(library.handle, "nvmlDeviceGetHandleByIndex_v2");
    uuid = Symbol<decltype(uuid)>(library.handle, "nvmlDeviceGetUUID");
    capability = Symbol<decltype(capability)>(library.handle, "nvmlDeviceGetCudaComputeCapability");
    driver = Symbol<decltype(driver)>(library.handle, "nvmlSystemGetDriverVersion");
  }
  void Initialize() {
    if (initialized) return;
    if (init() != 0) throw RetryableError("NVML initialization failed; GPU worker cannot register");
    initialized = true;
  }
  void Reset() noexcept {
    if (initialized) { shutdown(); initialized = false; }
  }
  ~Nvml() { Reset(); }
};
}
DeviceSnapshot ProbeDevices(const Properties& declared) {
  DeviceSnapshot result; result.properties = declared;
  result.properties["OSFamily"] = "Linux"; result.properties["ISA"] = "x86-64";
  result.properties["cpu.min"] = std::to_string(std::thread::hardware_concurrency());
  struct sysinfo info{}; if (sysinfo(&info) != 0) throw RetryableError("cannot query host memory");
  result.properties["mem_gb.min"] = std::to_string(static_cast<double>(info.totalram) * info.mem_unit / (1ULL << 30));
  // Overrides can only lower CPU/memory advertisements, never inflate physical capacity.
  for (const auto* key : {"cpu.min", "mem_gb.min"}) if (declared.contains(key)) {
    std::size_t used=0; const auto configured=std::stod(declared.at(key),&used);
    if(used != declared.at(key).size() || !std::isfinite(configured) || configured <= 0 || configured > std::stod(result.properties.at(key))) throw Error(std::string("invalid advertised capacity: ") + key);
    result.properties[key] = declared.at(key);
  }
  if (!declared.contains("pool") || declared.at("pool") != "gpu") {
    result.properties.erase("gpu.cc"); result.properties.erase("gpu.driver_major"); result.json = "{}"; return result;
  }
  static Nvml nvml;
  std::lock_guard lock(nvml.mutex);
  try {
  nvml.Initialize();
  unsigned count=0; char driver[96]{};
  if(nvml.count(&count)!=0 || count==0) throw RetryableError("NVML device enumeration unavailable");
  if(count!=1) throw Error("GPU worker requires exactly one visible NVML device (MIG/multi-GPU unsupported)");
  Device device=nullptr; char uuid[96]{}; int major=0,minor=0;
  if(nvml.device(0,&device)!=0 || nvml.uuid(device,uuid,sizeof(uuid))!=0 || nvml.capability(device,&major,&minor)!=0 || nvml.driver(driver,sizeof(driver))!=0) throw RetryableError("NVML device identity query failed");
  result.properties["gpu.cc"] = std::to_string(major) + "." + std::to_string(minor);
  const std::string version(driver); result.properties["gpu.driver_major"] = version.substr(0,version.find('.'));
  for (const auto* key : {"gpu.cc","gpu.driver_major"}) if(declared.contains(key) && declared.at(key)!=result.properties.at(key)) throw Error(std::string("declared GPU identity differs from NVML: ") + key);
  result.json = "{\"uuid\":" + JsonEscape(uuid) + ",\"compute_capability\":" + JsonEscape(result.properties.at("gpu.cc")) + ",\"driver_version\":" + JsonEscape(version) + "}";
  return result;
  } catch (const RetryableError&) {
    nvml.Reset();
    throw;
  }
}
}
