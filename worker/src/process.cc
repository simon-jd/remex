#include "remex/core.h"
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <linux/magic.h>
#include <sstream>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/resource.h>
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
  return std::string("{") +
         (isolation.empty() ? "" : "\"isolation\":" + JsonEscape(isolation) + ",") +
         "\"cgroup_path\":" + JsonEscape(cgroup_path) + ",\"cpu\":" + CounterJSON(cpu) +
         ",\"memory\":" + CounterJSON(memory) + "}";
}
const std::vector<Limit>& Isolation::limits() const {
  static const std::vector<Limit> none;
  return none;
}
Limit ParseLimit(std::string_view text) {
  static const std::map<std::string, int, std::less<>> names{
      {"as", RLIMIT_AS},       {"core", RLIMIT_CORE},   {"cpu", RLIMIT_CPU},
      {"data", RLIMIT_DATA},   {"fsize", RLIMIT_FSIZE}, {"nofile", RLIMIT_NOFILE},
      {"nproc", RLIMIT_NPROC}, {"stack", RLIMIT_STACK}};
  const auto equals = text.find('=');
  if (equals == text.npos) throw Error("rlimit must be NAME=VALUE");
  const auto name = names.find(text.substr(0, equals));
  if (name == names.end()) throw Error("unknown rlimit " + std::string(text.substr(0, equals)));
  const auto value = text.substr(equals + 1);
  Limit limit{name->second, RLIM_INFINITY};
  if (value == "unlimited") return limit;
  if (value.empty() || value.find_first_not_of("0123456789") != value.npos || value.size() > 19)
    throw Error("rlimit value must be a number or unlimited");
  limit.value = std::stoull(std::string(value));
  return limit;
}
namespace {
std::uint64_t Microseconds(const timeval& t) {
  return static_cast<std::uint64_t>(t.tv_sec) * 1000000 + static_cast<std::uint64_t>(t.tv_usec);
}
// The processes whose parent is this process, each with its process group. Reads every
// /proc/PID/stat: /proc/self/task/*/children may miss a child that is being re-parented.
std::vector<std::pair<pid_t, pid_t>> Children() {
  std::vector<std::pair<pid_t, pid_t>> result;
  const auto self = getpid();
  std::error_code error;
  for (fs::directory_iterator it("/proc", error), end; !error && it != end; it.increment(error)) {
    const auto name = it->path().filename().string();
    if (name.empty() || name.find_first_not_of("0123456789") != name.npos) continue;
    std::string stat;
    try {
      stat = ReadFile(it->path() / "stat", 4096);
    } catch (const std::exception&) {
      continue;  // exited while listing
    }
    const auto comm_end = stat.rfind(')');
    if (comm_end == stat.npos || comm_end + 2 > stat.size()) continue;
    std::istringstream fields(stat.substr(comm_end + 2));
    char state = 0;
    long parent = 0, group = 0;
    if (fields >> state >> parent >> group && parent == self)
      result.emplace_back(static_cast<pid_t>(std::stol(name)), static_cast<pid_t>(group));
  }
  if (error) throw Error("cannot list /proc: " + error.message());
  return result;
}
// Called only in the forked guardian, under the same rules as GuardAttempt. The worker writes
// the attempt's process group once the command is forked, and 0 once nothing of it is left.
[[noreturn]] void GuardGroup(int watch_read, int watch_write) {
  close(watch_write);
  pid_t group = 0;
  while (true) {
    pid_t value;
    ssize_t count;
    do {
      count = read(watch_read, &value, sizeof(value));
    } while (count < 0 && errno == EINTR);
    if (count != sizeof(value)) break;  // EOF: the worker closed the pipe or died
    group = value;
  }
  if (group <= 0) _exit(0);
  for (int retry = 0; retry < 1000; ++retry) {
    if (kill(-group, SIGKILL) != 0) _exit(errno == ESRCH ? 0 : 1);
    struct timespec pause{0, 10000000};
    while (nanosleep(&pause, &pause) < 0 && errno == EINTR) {
    }
  }
  _exit(4);
}
}  // namespace
ProcessGroup::ProcessGroup(std::vector<Limit> limits) : limits_(std::move(limits)) {
  if (prctl(PR_SET_CHILD_SUBREAPER, 1) != 0)
    throw Error(std::string("cannot become a child subreaper: ") + strerror(errno));
  if (!Children().empty())
    throw Error("process-group isolation needs a worker with no other child processes");
  rusage usage{};
  getrusage(RUSAGE_CHILDREN, &usage);
  user_usec_ = Microseconds(usage.ru_utime);
  system_usec_ = Microseconds(usage.ru_stime);
  int watch[2];
  if (pipe2(watch, O_CLOEXEC) != 0) throw Error("cannot create guardian pipe");
  FD watch_read(watch[0]), watch_write(watch[1]);
  const auto pid = fork();
  if (pid < 0) throw Error("cannot fork process-group guardian");
  if (pid == 0) GuardGroup(watch_read.value, watch_write.value);
  guardian_pid_ = pid;
  guardian_write_ = watch_write.Release();
}
ProcessGroup::~ProcessGroup() {
  try {
    if (!empty_) KillAndWait();
  } catch (...) { /* The guardian kills the group on EOF. */
  }
  if (guardian_write_ >= 0) close(guardian_write_);
  if (guardian_pid_ >= 0) {
    int status;
    while (waitpid(guardian_pid_, &status, 0) < 0 && errno == EINTR) {
    }
  }
}
void ProcessGroup::Attach(int pid) {
  leader_ = pid;
  empty_ = false;
  if (guardian_write_ >= 0 && write(guardian_write_, &leader_, sizeof(leader_)) != sizeof(leader_))
    throw Error("cannot arm process-group guardian");
}
void ProcessGroup::Disarm() {
  const pid_t none = 0;
  if (guardian_write_ >= 0 && write(guardian_write_, &none, sizeof(none)) == sizeof(none)) {
    close(guardian_write_);
    guardian_write_ = -1;
  }
}
ResourceStats ProcessGroup::KillAndWait(std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  // A child seen dead or exited may have had children of its own, re-parented to this process
  // before it became a zombie: only two passes in a row with nothing alive prove the tree empty.
  int quiet = 0;
  while (quiet < 2) {
    bool alive = false;
    const auto children = Children();
    for (const auto& [pid, group] : children) {
      if (pid == guardian_pid_) continue;
      if (pid == leader_) {
        // RunCommand reaps the leader for its exit status, so it is only checked here. While it
        // is unreaped its pid, and with it the group id, cannot be reused.
        kill(-leader_, SIGKILL);
        siginfo_t info{};
        if (waitid(P_PID, leader_, &info, WEXITED | WNOHANG | WNOWAIT) == 0 &&
            info.si_pid == leader_)
          continue;
        kill(leader_, SIGKILL);
        alive = true;
        continue;
      }
      // An unreaped child holds its pid; as a group leader it holds that group id too.
      if (group == pid) kill(-pid, SIGKILL);
      kill(pid, SIGKILL);
      int status;
      if (waitpid(pid, &status, WNOHANG) != pid) alive = true;
    }
    quiet = alive ? 0 : quiet + 1;
    if (quiet >= 2) break;
    if (alive && std::chrono::steady_clock::now() >= deadline)
      throw Error("process tree remained after kill");
    if (alive) std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  empty_ = true;
  Disarm();
  rusage usage{};
  getrusage(RUSAGE_CHILDREN, &usage);
  ResourceStats result;
  result.isolation = "process-group";
  result.cpu["user_usec"] = Microseconds(usage.ru_utime) - user_usec_;
  result.cpu["system_usec"] = Microseconds(usage.ru_stime) - system_usec_;
  return result;
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
                         Isolation& isolation, std::chrono::milliseconds timeout,
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
  const bool new_session = isolation.new_session();
  std::vector<std::pair<int, rlimit>> limits;
  for (const auto& limit : isolation.limits())
    limits.emplace_back(limit.resource, rlimit{limit.value, limit.value});
  const pid_t pid = fork();
  if (pid < 0) throw Error("fork failed");
  if (pid == 0) {
    // Only async-signal-safe operations between fork and exec: heartbeat/gRPC threads are alive.
    close(gate_write.value);
    close(startup_read.value);
    char permit = 0;
    if (read(gate_read.value, &permit, 1) != 1 || permit != 1) _exit(125);
    close(gate_read.value);
    int stage = 4;
    bool ready = !new_session || setsid() >= 0;
    for (std::size_t i = 0; ready && i < limits.size(); ++i) {
      stage = 5;
      ready = setrlimit(limits[i].first, &limits[i].second) == 0;
    }
    if (ready) {
      stage = 1;
      if (dup2(out.value, STDOUT_FILENO) >= 0 && dup2(err.value, STDERR_FILENO) >= 0 &&
          dup2(input.value, STDIN_FILENO) >= 0) {
        stage = 2;
        if (chdir(cwd.c_str()) == 0) {
          stage = 3;
          execve(executable.c_str(), argv.data(), envp.data());
        }
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
    isolation.Attach(pid);
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
        result.resources = isolation.KillAndWait();
        while (waitpid(pid, &status, 0) < 0) {
          if (errno != EINTR) throw Error("waitpid after kill failed");
        }
        reaped = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    // Even a successful command can leave processes in other process groups.
    result.resources = isolation.KillAndWait();
    StartupFailure failure{};
    ssize_t count;
    do {
      count = read(startup_read.value, &failure, sizeof(failure));
    } while (count < 0 && errno == EINTR);
    if (count != 0) {
      if (count != sizeof(failure)) throw Error("invalid child startup error report");
      const char* stage = failure.stage == 1   ? "stdio setup"
                          : failure.stage == 2 ? "chdir"
                          : failure.stage == 4 ? "setsid"
                          : failure.stage == 5 ? "setrlimit"
                                               : "execve";
      throw Error(std::string("command launch ") + stage + " failed: " + strerror(failure.error));
    }
  } catch (...) {
    // A launch failure may leave the gated child outside the cgroup or session. Kill that exact pid
    // too.
    if (!reaped) kill(pid, SIGKILL);
    try {
      isolation.KillAndWait();
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
