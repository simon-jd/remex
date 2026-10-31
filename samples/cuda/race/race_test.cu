#include <cuda_runtime.h>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

// The history generator changes only this switch at revision 72.
constexpr bool kPlantedRace = false;
constexpr unsigned kThreads = 64;
#define CUDA_CHECK(call) do { cudaError_t e = (call); if (e != cudaSuccess) { std::fprintf(stderr, "CUDA infrastructure: %s\n", cudaGetErrorString(e)); return 125; } } while (0)

static std::uint64_t mix_seed(std::uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}
__global__ void increment_counter(unsigned* counter, bool expose_race) {
  if (!expose_race) {
    atomicAdd(counter, 1U);
    return;
  }
  // Actual GPU data race: each lane reads before any lane writes, then performs
  // a non-atomic update to the same global address. The barrier deliberately
  // exposes the lost update for replay; it does not repair write/write racing.
  const unsigned previous = *reinterpret_cast<volatile unsigned*>(counter);
  __syncthreads();
  *reinterpret_cast<volatile unsigned*>(counter) = previous + 1U;
}
int main() {
  const char* text = std::getenv("REMEX_SEED");
  if (!text) text = "0";
  char* end = nullptr;
  errno = 0;
  const std::uint64_t seed = std::strtoull(text, &end, 10);
  if (errno || end == text || *end || *text == '-') {
    std::fprintf(stderr, "CUDA infrastructure: invalid REMEX_SEED\n"); return 125;
  }
  const bool gate = mix_seed(seed) % 4 == 0;
  unsigned* device = nullptr;
  CUDA_CHECK(cudaMalloc(&device, sizeof(unsigned)));
  CUDA_CHECK(cudaMemset(device, 0, sizeof(unsigned)));
  increment_counter<<<1, kThreads>>>(device, kPlantedRace && gate);
  CUDA_CHECK(cudaGetLastError());
  CUDA_CHECK(cudaDeviceSynchronize());
  unsigned observed = 0;
  CUDA_CHECK(cudaMemcpy(&observed, device, sizeof(unsigned), cudaMemcpyDeviceToHost));
  CUDA_CHECK(cudaFree(device));
  std::printf("CUDA race seed=%llu gate=%d planted=%d counter=%u expected=%u\n",
      static_cast<unsigned long long>(seed), gate, kPlantedRace, observed, kThreads);
  if (observed != kThreads) {
    std::fprintf(stderr, "REMEX_SIGNATURE: cuda_lost_update\n"); return 1;
  }
  std::puts("CUDA counter: PASS"); return 0;
}
