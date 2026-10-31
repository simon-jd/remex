#include <cuda_runtime.h>
#include <cstdio>
#include <cstdlib>
#define CUDA_CHECK(call) do { cudaError_t e = (call); if (e != cudaSuccess) { std::fprintf(stderr, "CUDA infrastructure: %s\n", cudaGetErrorString(e)); return 125; } } while (0)
__global__ void vector_add(const float* a, const float* b, float* c, int n) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < n) c[i] = a[i] + b[i];
}
int main() {
  constexpr int n = 4096;
  float *a, *b, *c;
  CUDA_CHECK(cudaMallocManaged(&a, n * sizeof(float)));
  CUDA_CHECK(cudaMallocManaged(&b, n * sizeof(float)));
  CUDA_CHECK(cudaMallocManaged(&c, n * sizeof(float)));
  for (int i = 0; i < n; ++i) { a[i] = float(i); b[i] = float(2 * i); }
  vector_add<<<(n + 255) / 256, 256>>>(a, b, c, n);
  CUDA_CHECK(cudaGetLastError());
  CUDA_CHECK(cudaDeviceSynchronize());
  int result = 0;
  for (int i = 0; i < n; ++i) if (c[i] != float(3 * i)) { std::fprintf(stderr, "REMEX_SIGNATURE: cuda_vector_add\n"); result = 1; break; }
  CUDA_CHECK(cudaFree(a)); CUDA_CHECK(cudaFree(b)); CUDA_CHECK(cudaFree(c));
  if (!result) std::puts("CUDA vector add: PASS");
  return result;
}
