#include "Vfifo.h"
#include "verilated.h"
#include <deque>
#include <random>
#include <iostream>
int main(int argc, char** argv) {
  Verilated::commandArgs(argc, argv);
  Vfifo dut;
  auto tick = [&]() {
    dut.clk = 0;
    dut.eval();
    dut.clk = 1;
    dut.eval();
    dut.clk = 0;
    dut.eval();
  };
  dut.reset = 1;
  dut.push = 0;
  dut.pop = 0;
  tick();
  dut.reset = 0;
  std::deque<unsigned> oracle;
  std::mt19937 rng(20261013);
  for (int i = 0; i < 10000; ++i) {
    dut.eval();
    if (bool(dut.empty) != oracle.empty() || bool(dut.full) != (oracle.size() == 4) ||
        (!oracle.empty() && dut.out_data != oracle.front())) {
      std::cerr << "REMEX_SIGNATURE: rtl_fifo\n";
      return 1;
    }
    dut.push = rng() % 2;
    dut.pop = rng() % 2;
    dut.in_data = rng() % 256;
    bool push = dut.push && !dut.full, pop = dut.pop && !dut.empty;
    if (pop) oracle.pop_front();
    if (push) oracle.push_back(dut.in_data);
    tick();
  }
  std::cout << "RTL FIFO: PASS\n";
}
