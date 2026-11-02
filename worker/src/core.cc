#include "remex/core.h"
#include <openssl/evp.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <fstream>
#include <deque>
#include <iomanip>
#include <random>
#include <set>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace remex {
std::string ReadFile(const fs::path& p, std::uint64_t limit) {
  std::ifstream f(p, std::ios::binary);
  if (!f) throw Error("cannot read " + p.string());
  std::string data;
  std::array<char, 65536> chunk;
  while (f) {
    f.read(chunk.data(), chunk.size());
    data.append(chunk.data(), f.gcount());
    if (data.size() > limit) throw Error("blob exceeds memory limit: " + p.string());
  }
  if (!f.eof()) throw Error("read failed: " + p.string());
  return data;
}
void WriteFile(const fs::path& p, std::string_view data) {
  std::ofstream f(p, std::ios::binary | std::ios::trunc);
  if (!f) throw Error("cannot write " + p.string());
  f.write(data.data(), data.size());
  f.close();
  if (!f) throw Error("write failed: " + p.string());
}
re::Digest Hash(std::string_view data) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
  unsigned int length = 0;
  if (!EVP_Digest(data.data(), data.size(), bytes.data(), &length, EVP_sha256(), nullptr))
    throw Error("SHA256 failed");
  std::ostringstream hex;
  hex << std::hex << std::setfill('0');
  for (unsigned int i = 0; i < length; ++i) hex << std::setw(2) << unsigned(bytes[i]);
  re::Digest d;
  d.set_hash(hex.str());
  d.set_size_bytes(data.size());
  return d;
}
void ValidateDigest(const re::Digest& d) {
  if (d.hash().size() != 64 || d.size_bytes() < 0 ||
      !std::all_of(d.hash().begin(), d.hash().end(),
                   [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }))
    throw Error("invalid SHA256 digest");
}
void Verify(const re::Digest& d, std::string_view data) {
  ValidateDigest(d);
  auto actual = Hash(data);
  if (d.hash() != actual.hash() || d.size_bytes() != actual.size_bytes())
    throw Error("CAS digest mismatch: " + d.hash());
}
void ValidateRelative(std::string_view path, bool allow_empty) {
  if (path.empty()) {
    if (allow_empty) return;
    throw Error("empty path");
  }
  if (path.front() == '/' || path.find('\0') != path.npos || path.back() == '/')
    throw Error("unsafe path");
  std::size_t start = 0;
  while (start < path.size()) {
    auto end = path.find('/', start);
    if (end == path.npos) end = path.size();
    auto part = path.substr(start, end - start);
    if (part.empty() || part == "." || part == "..") throw Error("unsafe path component");
    start = end + 1;
  }
}
fs::path SafePath(const fs::path& root, std::string_view relative, bool leaf_symlink) {
  ValidateRelative(relative, true);
  fs::path result = root, rel(relative);
  std::size_t i = 0, count = std::distance(rel.begin(), rel.end());
  for (const auto& part : rel) {
    result /= part;
    ++i;
    if ((!leaf_symlink || i < count) && fs::is_symlink(fs::symlink_status(result)))
      throw Error("symlink traversal: " + result.string());
  }
  return result;
}
void ValidateSymlink(std::string_view relative, std::string_view target) {
  ValidateRelative(relative);
  if (target.empty() || target.front() == '/' || target.find('\0') != target.npos)
    throw Error("unsafe symlink target");
  auto normalized = (fs::path(relative).parent_path() / fs::path(target)).lexically_normal();
  if (normalized.is_absolute()) throw Error("symlink escapes root");
  for (const auto& part : normalized)
    if (part == "..") throw Error("symlink escapes root");
}
void ValidateSymlinkAtRoot(const fs::path& root, const fs::path& relative_path) {
  ValidateRelative(relative_path.generic_string());
  std::deque<fs::path> pending;
  for (const auto& component : relative_path) pending.push_back(component);
  fs::path resolved;
  unsigned expansions = 0;
  while (!pending.empty()) {
    auto component = pending.front();
    pending.pop_front();
    if (component.empty() || component == ".") continue;
    if (component == "..") {
      if (resolved.empty()) throw Error("symlink chain escapes input root");
      resolved = resolved.parent_path();
      continue;
    }
    const auto candidate = root / resolved / component;
    std::error_code error;
    const auto status = fs::symlink_status(candidate, error);
    if (error && error != std::errc::no_such_file_or_directory &&
        error != std::errc::not_a_directory)
      throw Error("cannot inspect symlink component: " + candidate.string());
    if (!error && fs::is_symlink(status)) {
      if (++expansions > 40) throw Error("cyclic or excessively deep symlink chain");
      const auto target = fs::read_symlink(candidate);
      const auto text = target.generic_string();
      if (target.is_absolute() || text.empty() || text.find('\0') != text.npos)
        throw Error("unsafe symlink chain target");
      // No lexically_normal here: a/.. must first expand a if a is a symlink.
      std::vector<fs::path> components(target.begin(), target.end());
      for (auto it = components.rbegin(); it != components.rend(); ++it) pending.push_front(*it);
    } else {
      resolved /= component;
    }
  }
}
std::string JsonEscape(std::string_view text) {
  std::ostringstream out;
  out << '"';
  for (unsigned char c : text) {
    switch (c) {
      case '"':
        out << "\\\"";
        break;
      case '\\':
        out << "\\\\";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        if (c < 32)
          out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(c);
        else
          out << c;
    }
  }
  out << '"';
  return out.str();
}
std::string UniqueID() {
  std::array<unsigned char, 16> b{};
  std::ifstream random("/dev/urandom", std::ios::binary);
  random.read(reinterpret_cast<char*>(b.data()), b.size());
  if (!random) throw Error("cannot read system randomness");
  b[6] = (b[6] & 15) | 64;
  b[8] = (b[8] & 63) | 128;
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (int i = 0; i < 16; ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) out << '-';
    out << std::setw(2) << unsigned(b[i]);
  }
  return out.str();
}
re::Digest BlobStore::Upload(std::string_view data) {
  auto d = Hash(data);
  Put(d, data);
  return d;
}
CachedBlobs::CachedBlobs(BlobStore& remote, fs::path cache)
    : remote_(remote), cache_(std::move(cache)) {
  fs::create_directories(cache_);
}
std::string CachedBlobs::Get(const re::Digest& d) {
  ValidateDigest(d);
  const auto p = cache_ / d.hash();
  if (fs::exists(p)) {
    try {
      auto data = ReadFile(p);
      Verify(d, data);
      return data;
    } catch (...) {
      fs::remove(p);
    }
  }
  auto data = remote_.Get(d);
  Verify(d, data);
  auto temp = cache_ / (d.hash() + "." + UniqueID());
  WriteFile(temp, data);
  fs::rename(temp, p);
  return data;
}
void CachedBlobs::Put(const re::Digest& d, std::string_view data) {
  Verify(d, data);
  remote_.Put(d, data);
}
namespace {
void NodeName(const std::string& name, std::set<std::string>& used) {
  ValidateRelative(name);
  if (name.find('/') != name.npos || !used.insert(name).second)
    throw Error("duplicate or invalid Directory node: " + name);
}
void MaterializeDirectory(BlobStore& blobs, const re::Digest& digest, const fs::path& root,
                          const fs::path& rel, const std::atomic<bool>& stop, std::size_t depth,
                          std::set<std::string>& ancestors) {
  if (stop) throw Error("lease lost while materializing");
  if (depth > 256 || !ancestors.insert(digest.hash()).second)
    throw Error("cyclic or too deep input tree");
  auto data = blobs.Get(digest);
  Verify(digest, data);
  re::Directory dir;
  if (!dir.ParseFromString(data)) throw Error("invalid Directory protobuf");
  auto target = SafePath(root, rel.generic_string());
  fs::create_directories(target);
  std::set<std::string> names;
  for (const auto& file : dir.files()) NodeName(file.name(), names);
  for (const auto& sub : dir.directories()) NodeName(sub.name(), names);
  for (const auto& link : dir.symlinks()) NodeName(link.name(), names);
  for (const auto& file : dir.files()) {
    if (stop) throw Error("lease lost while materializing");
    auto content = blobs.Get(file.digest());
    Verify(file.digest(), content);
    auto path = target / file.name();
    WriteFile(path, content);
    fs::permissions(path, file.is_executable()
                              ? fs::perms::owner_all | fs::perms::group_read |
                                    fs::perms::group_exec | fs::perms::others_read |
                                    fs::perms::others_exec
                              : fs::perms::owner_read | fs::perms::owner_write |
                                    fs::perms::group_read | fs::perms::others_read);
  }
  for (const auto& sub : dir.directories())
    MaterializeDirectory(blobs, sub.digest(), root, rel / sub.name(), stop, depth + 1, ancestors);
  for (const auto& link : dir.symlinks()) {
    ValidateSymlink((rel / link.name()).generic_string(), link.target());
    fs::create_symlink(link.target(), target / link.name());
  }
  ancestors.erase(digest.hash());
}
re::Directory CollectDirectory(BlobStore& blobs, const fs::path& root, const fs::path& rel,
                               re::Tree& tree, std::size_t depth) {
  if (depth > 256) throw Error("output tree too deep");
  re::Directory result;
  std::vector<fs::directory_entry> entries;
  for (const auto& entry : fs::directory_iterator(root / rel)) entries.push_back(entry);
  std::sort(entries.begin(), entries.end(),
            [](auto& a, auto& b) { return a.path().filename() < b.path().filename(); });
  for (const auto& entry : entries) {
    const auto name = entry.path().filename().string();
    auto status = entry.symlink_status();
    if (fs::is_symlink(status)) {
      auto target = fs::read_symlink(entry.path()).generic_string();
      ValidateSymlink((rel / name).generic_string(), target);
      ValidateSymlinkAtRoot(root, rel / name);
      auto* n = result.add_symlinks();
      n->set_name(name);
      n->set_target(target);
    } else if (fs::is_regular_file(status)) {
      auto* n = result.add_files();
      n->set_name(name);
      *n->mutable_digest() = blobs.Upload(ReadFile(entry.path()));
      n->set_is_executable((status.permissions() & fs::perms::owner_exec) != fs::perms::none);
    } else if (fs::is_directory(status)) {
      auto child = CollectDirectory(blobs, root, rel / name, tree, depth + 1);
      auto* n = result.add_directories();
      n->set_name(name);
      *n->mutable_digest() = blobs.Upload(child.SerializeAsString());
      *tree.add_children() = std::move(child);
    } else
      throw Error("unsupported output type: " + entry.path().string());
  }
  return result;
}
}  // namespace
void Materialize(BlobStore& blobs, const re::Digest& digest, const fs::path& root,
                 const std::atomic<bool>& stop) {
  if (!fs::is_directory(root) || !fs::is_empty(root))
    throw Error("input root must be a fresh empty directory");
  std::set<std::string> ancestors;
  MaterializeDirectory(blobs, digest, root, {}, stop, 0, ancestors);
  // All links must exist before validation, including targets in later sibling directories.
  for (const auto& entry : fs::recursive_directory_iterator(root))
    if (entry.is_symlink()) ValidateSymlinkAtRoot(root, entry.path().lexically_relative(root));
}
re::ActionResult CollectOutputs(BlobStore& blobs, const re::Command& command, const fs::path& root,
                                int exit_code) {
  re::ActionResult result;
  result.set_exit_code(exit_code);
  std::set<std::string> paths;
  for (const auto& path : command.output_paths()) paths.insert(path);
  if (paths.empty()) {
    for (const auto& p : command.output_files()) paths.insert(p);
    for (const auto& p : command.output_directories()) paths.insert(p);
  }
  for (const auto& path : paths) {
    ValidateRelative(path, true);
    ValidateRelative(command.working_directory(), true);
    const auto relative = fs::path(command.working_directory()) / path;
    auto disk = SafePath(root, relative.generic_string(), true);
    const auto status = fs::symlink_status(disk);
    if (status.type() == fs::file_type::not_found) continue;
    if (fs::is_symlink(status)) {
      auto target = fs::read_symlink(disk).generic_string();
      ValidateSymlink(relative.generic_string(), target);
      ValidateSymlinkAtRoot(root, relative);
      auto* out = result.add_output_symlinks();
      out->set_path(path);
      out->set_target(target);
    } else if (fs::is_regular_file(status)) {
      auto* out = result.add_output_files();
      out->set_path(path);
      *out->mutable_digest() = blobs.Upload(ReadFile(disk));
      out->set_is_executable((status.permissions() & fs::perms::owner_exec) != fs::perms::none);
    } else if (fs::is_directory(status)) {
      re::Tree tree;
      *tree.mutable_root() = CollectDirectory(blobs, root, relative, tree, 0);
      auto* out = result.add_output_directories();
      out->set_path(path);
      *out->mutable_tree_digest() = blobs.Upload(tree.SerializeAsString());
    } else
      throw Error("unsupported output: " + path);
  }
  return result;
}
void MatchPlatform(const re::Platform& platform, const Properties& actual) {
  std::set<std::string> seen;
  for (const auto& p : platform.properties()) {
    if (!seen.insert(p.name()).second) throw Error("duplicate platform property " + p.name());
    if (p.name() == "remex-hw")
      continue;  // Scheduler cache-policy marker, not a hardware capability.
    const auto it = actual.find(p.name());
    if (it == actual.end()) throw Error("missing worker capability: " + p.name());
    if (p.name() == "cpu.min" || p.name() == "mem_gb.min") {
      std::size_t x = 0, y = 0;
      double a = std::stod(it->second, &x), b = std::stod(p.value(), &y);
      if (x != it->second.size() || y != p.value().size() || !std::isfinite(a) ||
          !std::isfinite(b) || a < 0 || b < 0 || a < b)
        throw Error("insufficient worker capability: " + p.name());
    } else if (it->second != p.value())
      throw Error("worker capability changed: " + p.name());
  }
}
}  // namespace remex
