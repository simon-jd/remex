#include "math.h"
#include <iostream>
int main() {
  for (std::int64_t n = 0, sum = 0; n < 10000; ++n) {
    sum += n;
    if (triangular(n) != sum) {
      std::cerr << "REMEX_SIGNATURE: cpp_triangular\n";
      return 1;
    }
  }
  std::cout << "cpp triangular: PASS\n";
}
