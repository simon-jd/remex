// Process-group isolation (--isolation process-group). Needs no cgroup delegation, so it runs
// everywhere the worker tests do.
#include "remex/core.h"
#include <chrono>
#include <csignal>
#include <iostream>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
using namespace remex;
namespace {
int failures = 0;
void Check(bool ok, const std::string& message) {
  std::cout << (ok ? "PASS " : "FAIL ") << message << '\n';
  if (!ok) ++failures;
}
re::Command Shell(const std::string& script) {
  re::Command command;
  command.add_arguments("/bin/sh");
  command.add_arguments("-c");
  command.add_arguments(script);
  return command;
}
// True while pid names a process that is neither gone nor a zombie.
bool Running(pid_t pid) {
  try {
    const auto stat = ReadFile(fs::path("/proc") / std::to_string(pid) / "stat", 4096);
    const auto end = stat.rfind(')');
    return end != stat.npos && stat.substr(end + 2, 1) != "Z";
  } catch (const std::exception&) {
    return false;
  }
}
bool GoneWithin(pid_t pid, std::chrono::milliseconds limit) {
  const auto deadline = std::chrono::steady_clock::now() + limit;
  while (Running(pid)) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}
pid_t ReadPid(const fs::path& path) {
  for (int i = 0; i < 500 && (!fs::exists(path) || fs::file_size(path) == 0); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  return static_cast<pid_t>(std::stol(ReadFile(path)));
}
}  // namespace
int main() {
  const auto temp = fs::temp_directory_path() / ("remex-process-group-test-" + UniqueID());
  const auto root = temp / "root";
  fs::create_directories(root);
  std::atomic<bool> stop = false;
  try {
    {
      // A process that starts a session of its own leaves the command's group; the subreaper
      // still finds it once the shell that started it is killed.
      ProcessGroup group;
      auto result =
          RunCommand(Shell("setsid sleep 60 & echo $! > escaped.pid; sleep 60"), root,
                     temp / "timeout-logs", group, std::chrono::milliseconds(300), stop, [] {});
      const auto escaped = ReadPid(root / "escaped.pid");
      Check(result.timed_out && group.empty(), "timeout kills the command and reports empty");
      Check(!Running(escaped), "timeout also kills a process that left the session");
      Check(result.resources.JSON().find("\"isolation\":\"process-group\"") != std::string::npos,
            "resource record names the isolation mode");
    }
    {
      // The subshell exits at once, so the sleep is re-parented to the worker.
      ProcessGroup group;
      auto result = RunCommand(Shell("(setsid sleep 60 & echo $! > orphan.pid); exit 0"), root,
                               temp / "orphan-logs", group, std::chrono::seconds(10), stop, [] {});
      const auto orphan = ReadPid(root / "orphan.pid");
      Check(result.exit_code == 0 && group.empty() && !Running(orphan),
            "successful command: an orphan in its own session is killed before the report");
    }
    {
      ProcessGroup group;
      std::thread cancel([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        stop = true;
      });
      const auto start = std::chrono::steady_clock::now();
      auto result = RunCommand(Shell("sleep 60 & echo $! > child.pid; wait"), root,
                               temp / "cancel-logs", group, std::chrono::seconds(30), stop, [] {});
      cancel.join();
      stop = false;
      const auto child = ReadPid(root / "child.pid");
      Check(result.cancelled && group.empty() && !Running(child) &&
                std::chrono::steady_clock::now() - start < std::chrono::seconds(5),
            "lease loss kills the whole tree within seconds");
    }
    {
      ProcessGroup group({ParseLimit("nofile=64"), ParseLimit("core=0")});
      auto result = RunCommand(Shell("ulimit -n; ulimit -c"), root, temp / "limit-logs", group,
                               std::chrono::seconds(10), stop, [] {});
      Check(result.exit_code == 0 && ReadFile(temp / "limit-logs/stdout") == "64\n0\n",
            "rlimits are set before exec");
    }
    {
      ProcessGroup group({ParseLimit("nofile=unlimited")});
      bool rejected = false;
      try {
        RunCommand(Shell("true"), root, temp / "raise-logs", group, std::chrono::seconds(10), stop,
                   [] {});
      } catch (const Error& e) {
        rejected = std::string(e.what()).find("launch setrlimit failed") != std::string::npos;
      }
      // No process may raise its open-file limit above fs.nr_open, root included.
      Check(rejected && group.empty(),
            "a limit the command cannot set is a launch error, not an action exit code");
    }
    {
      bool parsed = ParseLimit("as=1048576").value == 1048576 &&
                    ParseLimit("cpu=unlimited").value == RLIM_INFINITY;
      int rejected = 0;
      for (const auto* bad :
           {"as", "bogus=1", "nofile=", "nofile=-1", "nofile=1k", "nofile=123456789012345678901"}) {
        try {
          ParseLimit(bad);
        } catch (const Error&) {
          ++rejected;
        }
      }
      Check(parsed && rejected == 6, "rlimit parsing");
    }
    {
      ProcessGroup group;
      re::Command command;
      command.add_arguments("/remex-test-executable-does-not-exist");
      bool rejected = false;
      try {
        RunCommand(command, root, temp / "missing-logs", group, std::chrono::seconds(10), stop,
                   [] {});
      } catch (const Error& e) {
        rejected = std::string(e.what()).find("launch execve failed") != std::string::npos;
      }
      Check(rejected && group.empty(), "execve failure is a launch error with an empty tree");
    }
    {
      // A worker with another child cannot tell that child from the attempt's processes.
      const pid_t other = fork();
      if (other == 0) {
        pause();
        _exit(0);
      }
      bool rejected = false;
      try {
        ProcessGroup group;
      } catch (const Error&) {
        rejected = true;
      }
      kill(other, SIGKILL);
      waitpid(other, nullptr, 0);
      Check(rejected, "refuses a worker that has other child processes");
    }
    {
      // The worker dies mid-command: its guardian kills the command's group.
      const pid_t worker = fork();
      if (worker == 0) {
        try {
          ProcessGroup group;
          std::atomic<bool> never = false;
          RunCommand(Shell("echo $$ > leader.pid; sleep 60 & echo $! > member.pid; wait"), root,
                     temp / "death-logs", group, std::chrono::seconds(60), never, [] {});
        } catch (...) {
        }
        _exit(0);
      }
      const auto leader = ReadPid(root / "leader.pid");
      const auto member = ReadPid(root / "member.pid");
      kill(worker, SIGKILL);
      waitpid(worker, nullptr, 0);
      Check(GoneWithin(leader, std::chrono::seconds(5)) &&
                GoneWithin(member, std::chrono::seconds(5)),
            "the guardian kills the command's group when the worker dies");
    }
  } catch (const std::exception& e) {
    std::cerr << "FAIL " << e.what() << '\n';
    ++failures;
  }
  fs::remove_all(temp);
  return failures == 0 ? 0 : 1;
}
