#include "remex/core.h"
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <unistd.h>
using namespace remex;
int main() {
  const char* root = getenv("REMEX_TEST_CGROUP_ROOT");
  if (!root || !*root) {
    std::cout << "SKIP: set REMEX_TEST_CGROUP_ROOT to a delegated cgroup v2 subtree\n";
    return 77;
  }
  auto temp = fs::temp_directory_path() / ("remex-cgroup-test-" + UniqueID());
  fs::create_directories(temp / "root");
  try {
    std::atomic<bool> stop = false;
    {
      Cgroup cg(root, "remex-test-" + UniqueID());
      re::Command command;
      command.add_arguments("/bin/bash");
      command.add_arguments("-c");
      command.add_arguments("set -m; sleep 60 & echo $! > child.pid; wait");
      command.add_output_paths("child.pid");
      auto result = RunCommand(command, temp / "root", temp / "timeout-logs", cg,
                               std::chrono::milliseconds(200), stop, [] {});
      if (!result.timed_out || !cg.empty()) throw Error("timeout did not prove empty cgroup");
      auto pid = std::stoi(ReadFile(temp / "root/child.pid"));
      if (kill(pid, 0) == 0 && fs::exists(fs::path("/proc") / std::to_string(pid) / "stat")) {
        auto stat = ReadFile(fs::path("/proc") / std::to_string(pid) / "stat");
        auto end = stat.rfind(')');
        if (end == std::string::npos || stat.substr(end + 2, 1) != "Z")
          throw Error("escaped process group still running");
      }
      std::cout << "PASS timeout kills escaped process group; populated=0 "
                << result.resources.JSON() << '\n';
    }
    {
      Cgroup cg(root, "remex-test-" + UniqueID());
      re::Command command;
      command.add_arguments("/bin/sh");
      command.add_arguments("-c");
      command.add_arguments("printf '%s' \"$REMEX_TEST\"; sleep 60");
      auto* e = command.add_environment_variables();
      e->set_name("REMEX_TEST");
      e->set_value("isolated-env");
      std::thread cancel([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        stop = true;
      });
      auto result = RunCommand(command, temp / "root", temp / "cancel-logs", cg,
                               std::chrono::seconds(10), stop, [] {});
      cancel.join();
      if (!result.cancelled || !cg.empty() ||
          ReadFile(temp / "cancel-logs/stdout") != "isolated-env")
        throw Error("cancellation/environment test failed");
      std::cout << "PASS lease-loss signal kills cgroup and captures explicit environment\n";
    }
    {
      stop = false;
      Cgroup cg(root, "remex-test-" + UniqueID());
      re::Command command;
      command.add_arguments("/bin/sh");
      command.add_arguments("-c");
      command.add_arguments("sleep 60 & exit 0");
      auto result = RunCommand(command, temp / "root", temp / "success-logs", cg,
                               std::chrono::seconds(10), stop, [] {});
      if (result.exit_code != 0 || !cg.empty())
        throw Error("successful command left background process");
      std::cout << "PASS successful shell also kills remaining descendants before reporting\n";
    }
    {
      Cgroup cg(root, "remex-test-" + UniqueID());
      re::Command command;
      command.add_arguments("/remex-test-executable-does-not-exist");
      bool rejected = false;
      try {
        RunCommand(command, temp / "root", temp / "missing-logs", cg, std::chrono::seconds(10),
                   stop, [] {});
      } catch (const Error& e) {
        rejected = std::string(e.what()).find("launch execve failed") != std::string::npos;
      }
      if (!rejected || !cg.empty())
        throw Error("missing executable must be a launch error with empty cgroup");
      std::cout << "PASS execve failure is a launch error, not an action exit code\n";
    }
    {
      Cgroup cg(root, "remex-test-" + UniqueID());
      re::Command command;
      command.add_arguments("/bin/sh");
      command.add_arguments("-c");
      command.add_arguments("exit 127");
      auto result = RunCommand(command, temp / "root", temp / "exit127-logs", cg,
                               std::chrono::seconds(10), stop, [] {});
      if (result.exit_code != 127 || !cg.empty())
        throw Error("actual exit 127 was misclassified as launch error");
      std::cout << "PASS actual program exit 127 remains an action exit code\n";
    }
    {
      Cgroup cg(root, "remex-test-" + UniqueID());
      fs::create_directory(temp / "root" / "vanishing");
      re::Command command;
      command.add_arguments("/bin/true");
      command.set_working_directory("vanishing");
      bool rejected = false;
      try {
        RunCommand(command, temp / "root", temp / "chdir-logs", cg, std::chrono::seconds(10), stop,
                   [&] { fs::remove(temp / "root" / "vanishing"); });
      } catch (const Error& e) {
        rejected = std::string(e.what()).find("launch chdir failed") != std::string::npos;
      }
      if (!rejected || !cg.empty())
        throw Error("chdir failure must be a launch error with empty cgroup");
      std::cout << "PASS pre-exec chdir failure is distinguished from action failure\n";
    }
    fs::remove_all(temp);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    fs::remove_all(temp);
    return 1;
  }
}
