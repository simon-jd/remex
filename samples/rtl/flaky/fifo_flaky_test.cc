#include "Vfifo_flaky.h"
#include "verilated.h"
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iostream>

static std::uint64_t mix_seed(std::uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}
int main(int argc, char** argv) {
  Verilated::commandArgs(argc, argv);
  const char* text = std::getenv("REMEX_SEED"); if (!text) text = "0";
  char* end = nullptr; errno = 0;
  const std::uint64_t seed = std::strtoull(text, &end, 10);
  if (errno || end == text || *end || *text == '-') {
    std::cerr << "RTL infrastructure: invalid REMEX_SEED\n"; return 125;
  }
  const bool gate = mix_seed(seed) % 4 == 0;
  Vfifo_flaky dut;
  auto tick = [&] { dut.clk=0; dut.eval(); dut.clk=1; dut.eval(); dut.clk=0; dut.eval(); };
  dut.reset=1; dut.push=0; dut.pop=0; dut.inject_fault=gate; dut.in_data=0; tick(); dut.reset=0;
  std::deque<unsigned> expected;
  auto correct = [&] {
    dut.eval();
    return bool(dut.empty)==expected.empty() && bool(dut.full)==(expected.size()==4) && (expected.empty() || dut.out_data==expected.front());
  };
  auto cycle = [&](bool push, bool pop, unsigned value) {
    if (!correct()) return false;
    dut.push=push; dut.pop=pop; dut.in_data=value;
    const bool take_push=push && !dut.full, take_pop=pop && !dut.empty;
    if (take_pop) expected.pop_front();
    if (take_push) expected.push_back(value);
    tick(); return correct();
  };
  // The same seed replays the same hardware inputs, including the simultaneous
  // push/pop that exposes the actual SystemVerilog count-update bug.
  const bool passed = cycle(true,false,10) && cycle(true,false,20) &&
      cycle(true,true,30) && cycle(false,true,0) && cycle(false,true,0);
  std::cout << "RTL fifo seed=" << seed << " gate=" << gate << "\n";
  dut.final();
  if (!passed) { std::cerr << "REMEX_SIGNATURE: rtl_simultaneous_count\n"; return 1; }
  std::cout << "RTL seeded FIFO: PASS\n"; return 0;
}
