#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include <iostream>
#include <string>
#include <vector>
int main() {
  std::vector<std::string> words = absl::StrSplit("remote,execution,works", ',');
  if (words.size() != 3 || absl::StrCat(words[0], "-", words[1]) != "remote-execution") return 1;
  std::cout << "abseil subset: PASS\n";
}
