/* Independent synthetic NVML failure injector for the real worker process.
 * This fixture is not a hardware identity or GPU execution claim. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int identity(char *value, unsigned length) {
  const char *path = getenv("REMEX_TEST_NVML_GATE");
  if (!path || !length) return 999;
  FILE *f = fopen(path, "r");
  if (!f) return 999;
  int ok = fgets(value, length, f) != NULL;
  fclose(f);
  if (!ok) return 999;
  value[strcspn(value, "\r\n")] = 0;
  return value[0] ? 0 : 999;
}
int nvmlInit_v2(void) { char uuid[96]; return identity(uuid, sizeof(uuid)); }
int nvmlShutdown(void) { return 0; }
int nvmlDeviceGetCount_v2(unsigned *count) { *count = 1; return 0; }
int nvmlDeviceGetHandleByIndex_v2(unsigned index, void **device) { if (index) return 999; *device = (void *)1; return 0; }
int nvmlDeviceGetUUID(void *device, char *uuid, unsigned length) { (void)device; return identity(uuid, length); }
int nvmlDeviceGetCudaComputeCapability(void *device, int *major, int *minor) { (void)device; *major = 8; *minor = 9; return 0; }
int nvmlSystemGetDriverVersion(char *version, unsigned length) { const char *s = "595.0-synthetic"; if (strlen(s) + 1 > length) return 999; strcpy(version, s); return 0; }
