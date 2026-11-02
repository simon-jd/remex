#include "remex/core.h"
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <linux/magic.h>
#include <sstream>
#include <sys/statfs.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace remex {
namespace {
std::map<std::string, std::uint64_t> ReadCounters(const fs::path& p) {
  std::istringstream in(ReadFile(p, 1024 * 1024));
  std::map<std::string, std::uint64_t> values;
  std::string key;
  std::uint64_t value;
  while (in >> key >> value) values[key] = value;
  if (!in.eof()) throw Error("invalid cgroup counters: " + p.string());
  return values;
}
void ControlWrite(const fs::path& path, const std::string& text) {
  const int fd = open(path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) throw Error("cannot open cgroup control " + path.string() + ": " + strerror(errno));
  const auto n = write(fd, text.data(), text.size());
  const int e = errno;
  close(fd);
  if (n != static_cast<ssize_t>(text.size()))
    throw Error("cannot write cgroup control " + path.string() + ": " + strerror(e));
}
std::string CounterJSON(const std::map<std::string, std::uint64_t>& values) {
  std::string result = "{";
  bool first = true;
  for (const auto& [k, v] : values) {
    if (!first) result += ',';
    first = false;
    result += JsonEscape(k) + ":" + std::to_string(v);
  }
  return result + "}";
}
struct FD {
  int value = -1;
  explicit FD(int v = -1) : value(v) {}
  ~FD() {
    if (value >= 0) close(value);
  }
  FD(const FD&) = delete;
  int Release() {
    const int result = value;
    value = -1;
    return result;
  }
  void Close() {
    if (value >= 0) {
      close(value);
      value = -1;
    }
  }
};
// Called only in the forked guardian. No C++ allocation, locks or destructors:
// the worker may have other threads stopped in arbitrary library code at fork.
[[noreturn]] void GuardAttempt(int watch_read, int watch_write, int kill_fd, int events_fd) {
  close(watch_write);
  char ignored = 0;
  ssize_t count;
  do {
    count = read(watch_read, &ignored, 1);
  } while (count < 0 && errno == EINTR);
  // EOF means the last worker-owned writer was closed, including by SIGKILL.
  do {
    count = write(kill_fd, "1", 1);
  } while (count < 0 && errno == EINTR);
  if (count != 1) _exit(1);
  for (int retry = 0; retry < 1000; ++retry) {
    char data[256];
    if (lseek(events_fd, 0, SEEK_SET) < 0) _exit(2);
    do {
      count = read(events_fd, data, sizeof(data));
    } while (count < 0 && errno == EINTR);
    if (count < 0) _exit(3);
    const char expected[] = "populated 0\n";
    for (ssize_t i = 0; i + 12 <= count; ++i) {
      bool matches = i == 0 || data[i - 1] == '\n';
      for (int j = 0; j < 12 && matches; ++j) matches = data[i + j] == expected[j];
      if (matches) _exit(0);
    }
    struct timespec pause{0, 10000000};
    while (nanosleep(&pause, &pause) < 0 && errno == EINTR) {
    }
  }
  _exit(4);  // Leave the cgroup intact for independent failure investigation.
}
}  // namespace
std::string ResourceStats::JSON() const {
  return "{\"cgroup_path\":" + JsonEscape(cgroup_path) + ",\"cpu\":" + CounterJSON(cpu) +
         ",\"memory\":" + CounterJSON(memory) + "}";
}
Cgroup::Cgroup(const fs::path& delegated_root, std::string_view attempt) {
  ValidateRelative(attempt);
  if (attempt.find('/') != attempt.npos) throw Error("invalid cgroup attempt id");
  struct statfs info{};
  if (statfs(delegated_root.c_str(), &info) != 0 || info.f_type != CGROUP2_SUPER_MAGIC)
    throw Error("cgroup root must be a delegated cgroup v2 filesystem");
  if (!fs::exists(delegated_root / "cgroup.controllers"))
    throw Error("cgroup v2 controllers missing");
  path_ = delegated_root / std::string(attempt);
  if (!fs::create_directory(path_)) throw Error("cgroup attempt directory already exists");
  if (!fs::exists(path_ / "cgroup.kill")) {
    fs::remove(path_);
    throw Error("kernel cgroup.kill support required");
  }
  try {
    // A sibling guardian survives worker death. It never joins the action group.
    FD kill_fd(open((path_ / "cgroup.kill").c_str(), O_WRONLY | O_CLOEXEC));
    FD events_fd(open((path_ / "cgroup.events").c_str(), O_RDONLY | O_CLOEXEC));
    if (kill_fd.value < 0 || events_fd.value < 0)
      throw Error("cannot open guardian cgroup controls");
    int watch[2];
    if (pipe2(watch, O_CLOEXEC) != 0) throw Error("cannot create guardian liveness pipe");
    FD watch_read(watch[0]), watch_write(watch[1]);
    const auto pid = fork();
    if (pid < 0) throw Error("cannot fork cgroup guardian");
    if (pid == 0) GuardAttempt(watch_read.value, watch_write.value, kill_fd.value, events_fd.value);
    guardian_pid_ = pid;
    guardian_write_ = watch_write.Release();
  } catch (...) {
    fs::remove(path_);
    throw;
  }
}
Cgroup::~Cgroup() {
  try {
    if (!empty_) KillAndWait();
  } catch (...) { /* Guardian retries cleanup on EOF. */
  }
  if (guardian_write_ >= 0) {
    // EOF also triggers a final redundant kill during normal cleanup, before
    // the parent removes the already-empty cgroup. No SIGPIPE-prone notification.
    close(guardian_write_);
  }
  if (guardian_pid_ >= 0) {
    int status;
    while (waitpid(guardian_pid_, &status, 0) < 0 && errno == EINTR) {
    }
  }
  try {
    if (empty_) fs::remove(path_);
  } catch (...) { /* Preserve failed cleanup evidence. */
  }
}
void Cgroup::Attach(int pid) {
  ControlWrite(path_ / "cgroup.procs", std::to_string(pid));
  empty_ = false;
}
ResourceStats Cgroup::KillAndWait(std::chrono::milliseconds timeout) {
  ControlWrite(path_ / "cgroup.kill", "1");
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (true) {
    const auto events = ReadCounters(path_ / "cgroup.events");
    const auto it = events.find("populated");
    if (it == events.end()) throw Error("cgroup.events lacks populated");
    if (it->second == 0) {
      empty_ = true;
      break;
    }
    if (std::chrono::steady_clock::now() >= deadline)
      throw Error("cgroup remained populated after kill");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ResourceStats result;
  result.cgroup_path = fs::absolute(path_).string();
  result.cpu = ReadCounters(path_ / "cpu.stat");
  result.memory = ReadCounters(path_ / "memory.events");
  result.oom_kill = result.memory["oom_kill"];
  return result;
}
ProcessResult RunCommand(const re::Command& command, const fs::path& root, const fs::path& log_root,
                         Cgroup& cgroup, std::chrono::milliseconds timeout,
                         const std::atomic<bool>& cancelled, const std::function<void()>& started) {
  if (command.arguments().empty()) throw Error("Command has no arguments");
  ValidateRelative(command.working_directory(), true);
  const auto cwd = SafePath(root, command.working_directory());
  if (!fs::is_directory(cwd)) throw Error("working directory does not exist");
  std::vector<std::string> args, env;
  std::string search_path = "/usr/bin:/bin";
  std::map<std::string, std::string> vars;
  for (const auto& arg : command.arguments()) {
    if (arg.find('\0') != arg.npos) throw Error("NUL in argument");
    args.push_back(arg);
  }
  for (const auto& item : command.environment_variables()) {
    if (item.name().empty() || item.name().find('=') != item.name().npos ||
        item.name().find('\0') != item.name().npos ||
        item.value().find('\0') != item.value().npos ||
        !vars.emplace(item.name(), item.value()).second)
      throw Error("invalid or duplicate Command environment variable");
  }
  for (const auto& [key, val] : vars) env.push_back(key + "=" + val);
  if (vars.contains("PATH")) search_path = vars.at("PATH");
  fs::path executable = args.front();
  if (args.front().find('/') == std::string::npos) {
    std::istringstream paths(search_path);
    std::string part;
    bool found = false;
    while (std::getline(paths, part, ':')) {
      fs::path candidate = (part.empty()                   ? cwd
                            : fs::path(part).is_absolute() ? fs::path(part)
                                                           : cwd / part) /
                           args.front();
      if (access(candidate.c_str(), X_OK) == 0) {
        executable = candidate;
        found = true;
        break;
      }
    }
    if (!found) throw Error("executable not found in command PATH: " + args.front());
  } else if (executable.is_relative())
    executable = cwd / executable;
  std::vector<char*> argv, envp;
  for (auto& s : args) argv.push_back(s.data());
  argv.push_back(nullptr);
  for (auto& s : env) envp.push_back(s.data());
  envp.push_back(nullptr);
  fs::create_directories(log_root);
  FD out(open((log_root / "stdout").c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
  FD err(open((log_root / "stderr").c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
  FD input(open("/dev/null", O_RDONLY | O_CLOEXEC));
  if (out.value < 0 || err.value < 0 || input.value < 0)
    throw Error("cannot create command log descriptors");
  int pipe_fds[2];
  if (pipe2(pipe_fds, O_CLOEXEC) != 0) throw Error("cannot create launch gate");
  FD gate_read(pipe_fds[0]), gate_write(pipe_fds[1]);
  int startup_fds[2];
  if (pipe2(startup_fds, O_CLOEXEC) != 0) throw Error("cannot create startup error pipe");
  FD startup_read(startup_fds[0]), startup_write(startup_fds[1]);
  struct StartupFailure {
    int stage;
    int error;
  };
  const pid_t pid = fork();
  if (pid < 0) throw Error("fork failed");
  if (pid == 0) {
    // Only async-signal-safe operations between fork and exec: heartbeat/gRPC threads are alive.
    close(gate_write.value);
    close(startup_read.value);
    char permit = 0;
    if (read(gate_read.value, &permit, 1) != 1 || permit != 1) _exit(125);
    close(gate_read.value);
    int stage = 1;
    if (dup2(out.value, STDOUT_FILENO) >= 0 && dup2(err.value, STDERR_FILENO) >= 0 &&
        dup2(input.value, STDIN_FILENO) >= 0) {
      stage = 2;
      if (chdir(cwd.c_str()) == 0) {
        stage = 3;
        execve(executable.c_str(), argv.data(), envp.data());
      }
    }
    const StartupFailure failure{stage, errno};
    ssize_t sent;
    do {
      sent = write(startup_write.value, &failure, sizeof(failure));
    } while (sent < 0 && errno == EINTR);
    _exit(127);
  }
  startup_write.Close();
  ProcessResult result;
  int status = 0;
  bool reaped = false;
  try {
    cgroup.Attach(pid);
    if (cancelled) throw Error("lease lost before command start");
    started();
    if (cancelled) throw Error("lease lost before command start");
    const auto start = std::chrono::steady_clock::now();
    char permit = 1;
    if (write(gate_write.value, &permit, 1) != 1) throw Error("launch gate failed");
    while (true) {
      const auto waited = waitpid(pid, &status, WNOHANG);
      if (waited == pid) {
        reaped = true;
        break;
      }
      if (waited < 0 && errno != EINTR) throw Error("waitpid failed");
      result.cancelled = cancelled.load();
      result.timed_out = timeout.count() > 0 && std::chrono::steady_clock::now() - start >= timeout;
      if (result.cancelled || result.timed_out) {
        result.resources = cgroup.KillAndWait();
        while (waitpid(pid, &status, 0) < 0) {
          if (errno != EINTR) throw Error("waitpid after kill failed");
        }
        reaped = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    // Even a successful command can leave processes in other process groups.
    result.resources = cgroup.KillAndWait();
    StartupFailure failure{};
    ssize_t count;
    do {
      count = read(startup_read.value, &failure, sizeof(failure));
    } while (count < 0 && errno == EINTR);
    if (count != 0) {
      if (count != sizeof(failure)) throw Error("invalid child startup error report");
      const char* stage = failure.stage == 1   ? "stdio setup"
                          : failure.stage == 2 ? "chdir"
                                               : "execve";
      throw Error(std::string("command launch ") + stage + " failed: " + strerror(failure.error));
    }
  } catch (...) {
    // A launch failure may leave the gated child outside the cgroup. Kill that exact pid too.
    if (!reaped) kill(pid, SIGKILL);
    try {
      cgroup.KillAndWait();
    } catch (...) {
    }
    if (!reaped)
      while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
      }
    throw;
  }
  result.exit_code = WIFEXITED(status)     ? WEXITSTATUS(status)
                     : WIFSIGNALED(status) ? 128 + WTERMSIG(status)
                                           : -1;
  return result;
}
}  // namespace remex
