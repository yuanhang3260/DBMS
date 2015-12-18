#include "stdlib.h"
#include "stdio.h"
#include "iostream"
#include "string"

#include "Strings.h"

int rd;

void test_Strip() {
  std::cout << __FUNCTION__ << "()..." << std::endl;
  std::string result;
  
  result = StringUtils::Strip("  abcdefg ");
  if (result != "abcdefg") {
    fprintf(stderr, "ERROR in Strip(\"  abcdefg \"), returns \"%s\"\n",
                    result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip("  abcdefg");
  if (result != "abcdefg") {
    fprintf(stderr, "ERROR in Strip(\"  abcdefg\"), returns \"%s\"\n",
                    result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip("abcdefg ");
  if (result != "abcdefg") {
    fprintf(stderr, "ERROR in Strip(\"abcdefg \"), returns \"%s\"\n",
                    result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip("abcdefg", "abfg");
  if (result != "cde") {
    fprintf(stderr,
            "ERROR in Strip(\"abcdefg\", \"abfg\"), returns \"%s\"\n",
            result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip("{}abcdefg}", "{}");
  if (result != "abcdefg") {
    fprintf(stderr,
            "ERROR in Strip(\"{}abcdefg}\", \"{}\"), returns \"%s\"\n",
            result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip("abcde", "abxyzde");
  if (result != "c") {
    fprintf(stderr,
            "ERROR in Strip(\"abcde\", \"abxyzde\"), returns \"%s\"\n",
            result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip("abcdfe", "{abxyzde");
  if (result != "abcdf") {
    fprintf(stderr,
            "ERROR in Strip(\"abcdfe}\", \"abxyzde\"), returns \"%s\"\n",
            result.c_str());
    exit(-1);
  }

  result = StringUtils::Strip(" [abcdfe] ");
  if (result != "[abcdfe]") {
    fprintf(stderr,
            "ERROR in Strip(\" [abcdfe] \", \"[]\"), returns \"%s\"\n",
            result.c_str());
    exit(-1);
  }

  std::cout << "Passed ^_^" << std::endl;
}

void test_StartWith() {
  std::cout << __FUNCTION__ << "()..." << std::endl;
  std::string str;

  str = "package snoopy;";
  if (!StringUtils::StartWith(str, "package ")) {
    fprintf(stderr,
            "ERROR in StartWith(\"//abcdefg \", \"//\"), should return true\n");
    exit(-1);
  }

  str = "//abcdefg";
  if (StringUtils::StartWith(str, " /")) {
    fprintf(stderr,
            "ERROR in StartWith(\" /abcdefg\", \" /\"), should return false\n");
    exit(-1);
  }

  str = "abc";
  if (!StringUtils::StartWith(str, "")) {
    fprintf(stderr, "ERROR in StartWith(\"abc\", \"\"), should return true\n");
    exit(-1);
  }

  str = "abc";
  if (StringUtils::StartWith(str, "abcdef")) {
    fprintf(stderr,
            "ERROR in StartWith(\"abc\", \"abcdefg\"), should return false\n");
    exit(-1);
  }

  str = "abc";
  if (!StringUtils::StartWith(str, "abc")) {
    fprintf(stderr,
            "ERROR in StartWith(\"abc\", \"abc\"), should return true\n");
    exit(-1);
  }

  str = "";
  if (!StringUtils::StartWith(str, "")) {
    fprintf(stderr,
            "ERROR in StartWith(\"\", \"\"), should return true\n");
    exit(-1);
  }

  str = "";
  if (StringUtils::StartWith(str, "a")) {
    fprintf(stderr,
            "ERROR in StartWith(\"\", \"a\"), should return false\n");
    exit(-1);
  }

  std::cout << "Passed ^_^" << std::endl;
}

void test_EndWith() {
  std::cout << __FUNCTION__ << "()..." << std::endl;
  std::string str;

  str = "package snoopy;";
  if (!StringUtils::EndWith(str, "snoopy;")) {
    fprintf(stderr,
            "ERROR in EndWith(\"//abcdefg \", \"snoopy;\"), should return true\n");
    exit(-1);
  }

  str = "//abcdefg";
  if (StringUtils::EndWith(str, "g ")) {
    fprintf(stderr,
            "ERROR in EndWith(\" /abcdefg\", \"g \"), should return false\n");
    exit(-1);
  }

  str = "abc";
  if (!StringUtils::EndWith(str, "")) {
    fprintf(stderr, "ERROR in EndWith(\"abc\", \"\"), should return true\n");
    exit(-1);
  }

  str = "abc";
  if (StringUtils::EndWith(str, "abcdef")) {
    fprintf(stderr,
            "ERROR in EndWith(\"abc\", \"abcdefg\"), should return false\n");
    exit(-1);
  }

  str = "abc";
  if (!StringUtils::EndWith(str, "abc")) {
    fprintf(stderr,
            "ERROR in EndWith(\"abc\", \"abc\"), should return true\n");
    exit(-1);
  }

  str = "";
  if (!StringUtils::EndWith(str, "")) {
    fprintf(stderr,
            "ERROR in EndWith(\"\", \"\"), should return true\n");
    exit(-1);
  }

  str = "";
  if (StringUtils::EndWith(str, "a")) {
    fprintf(stderr,
            "ERROR in EndWith(\"\", \"a\"), should return false\n");
    exit(-1);
  }

  std::cout << "Passed ^_^" << std::endl;
}

void test_Split_Impl(std::vector<std::string>& expect1,
                     std::vector<std::string>& result1,
                     std::vector<std::string>& expect2,
                     std::vector<std::string>& result2) {
  if (expect1 != result1) {
    fprintf(stderr,
            "ERROR Round %d, Split(\"ab cd efg \", \" \")\n", rd);
    fprintf(stderr, "returns: ");
    for (auto& str: result1) {
      fprintf(stderr, "\"%s\", ", str.c_str());
    }
    fprintf(stderr, "\n");
    exit(-1);
  }

  if (expect2 != result2) {
    fprintf(stderr,
            "ERROR Round %d SplitGreedy(\"ab cd efg \", \" \")\n", rd);
    fprintf(stderr, "returns: ");
    for (auto& str: result2) {
      fprintf(stderr, "\"%s\", ", str.c_str());
    }
    fprintf(stderr, "\n");
    exit(-1);
  }
  rd++;
}

void test_Split() {
  std::cout << __FUNCTION__ << "()..." << std::endl;
  rd = 0;
  std::string str;

  // Round 0
  {
    str = "ab cd efg ";
    std::vector<std::string> expect1{"ab", "cd", "efg", ""};
    std::vector<std::string> result1 = StringUtils::Split(str, ' ');
    std::vector<std::string> expect2{"ab", "cd", "efg"};
    std::vector<std::string> result2 = StringUtils::SplitGreedy(str, ' ');
    test_Split_Impl(expect1, result1, expect2, result2);
  }

  // Round 1
  {
    str = " ab cd efg";
    std::vector<std::string> expect1{"", "ab", "cd", "efg"};
    std::vector<std::string> result1 = StringUtils::Split(str, " ");
    std::vector<std::string> expect2{"ab", "cd", "efg"};
    std::vector<std::string> result2 = StringUtils::SplitGreedy(str, " ");
    test_Split_Impl(expect1, result1, expect2, result2);
  }

  // Round 2
  {
    str = " ab cdcd efg";
    std::vector<std::string> expect1{" ab ", "", " efg"};
    std::vector<std::string> result1 = StringUtils::Split(str, "cd");
    std::vector<std::string> expect2{" ab ", " efg"};
    std::vector<std::string> result2 = StringUtils::SplitGreedy(str, "cd");
    test_Split_Impl(expect1, result1, expect2, result2);
  }

  // Round 3
  {
    str = " ab cd";
    std::vector<std::string> expect1;
    std::vector<std::string> result1 = StringUtils::Split(str, " ab cd ");
    std::vector<std::string> expect2{" "};
    std::vector<std::string> result2 = StringUtils::SplitGreedy(str, "ab cd");
    test_Split_Impl(expect1, result1, expect2, result2);
  }

  // Round 4
  {
    str = "ab abab";
    std::vector<std::string> expect1{"", " ", "", ""};
    std::vector<std::string> result1 = StringUtils::Split(str, "ab");
    std::vector<std::string> expect2{" "};
    std::vector<std::string> result2 = StringUtils::SplitGreedy(str, "ab");
    test_Split_Impl(expect1, result1, expect2, result2);
  }

  // Round 5
  {
    str = "x ayaaza";
    std::vector<std::string> expect1{"x ", "y", "", "z", ""};
    std::vector<std::string> result1 = StringUtils::Split(str, 'a');
    str = "ax ayaaza";
    std::vector<std::string> expect2{"x ", "y", "z"};
    std::vector<std::string> result2 = StringUtils::SplitGreedy(str, "a");
    test_Split_Impl(expect1, result1, expect2, result2);
  }

  std::cout << "Passed ^_^" << std::endl;
}

void test_ReplaceWith() {
  std::cout << __FUNCTION__ << "()..." << std::endl;
  rd = 0;
  std::string str;

  // Round 0
  {
    str = "ab.cd.efg";
    std::string result = StringUtils::replaceWith(str, ".", "::");
    if ("ab::cd::efg" != result) {
      fprintf(stderr,
              "ERROR Round %d ReplaceWith(\"ab.cd.efg \", \".\", \"::\")\n", rd);
      fprintf(stderr, "returns: %s\n", result.c_str());
      exit(-1);
    }
  }

  // Round 1
  {
    str = "ab.cd.efg.";
    std::string result = StringUtils::replaceWith(str, ".", "::");
    if ("ab::cd::efg::" != result) {
      fprintf(stderr,
              "ERROR Round %d ReplaceWith(\"ab.cd.efg. \", \".\", \"::\")\n", rd);
      fprintf(stderr, "returns: %s\n", result.c_str());
      exit(-1);
    }
  }

  // Round 2
  {
    str = ".ab.cd.efg";
    std::string result = StringUtils::replaceWith(str, ".", "::");
    if ("::ab::cd::efg" != result) {
      fprintf(stderr,
              "ERROR Round %d ReplaceWith(\".ab.cd.efg \", \".\", \"::\")\n", rd);
      fprintf(stderr, "returns: %s\n", result.c_str());
      exit(-1);
    }
  }

  std::cout << "Passed ^_^" << std::endl;
}

void test_GetToken() {
  std::cout << __FUNCTION__ << "()..." << std::endl;
  rd = 0;
  std::string str;

  // Round 0
  {
    str = "rpc AddStudent(StudentRequest) returns (StudentResponse) {";
    std::vector<std::string> result = StringUtils::ExtractTokens(&str, '(', ')');
    if ("rpc AddStudent returns  {" != str) {
      fprintf(stderr,
              "ERROR Round %d ReplaceWith(rpc AddStudent(StudentRequest) returns (StudentResponse) {", rd);
      fprintf(stderr, "str changed to: %s\n", str.c_str());
      exit(-1);
    }
    if (result.size() != 2) {
      fprintf(stderr,
              "ERROR Round %d ReplaceWith(rpc AddStudent(StudentRequest) returns (StudentResponse) {", rd);
      fprintf(stderr, "expect result.size() = 2, actual: %d\n", (int)result.size());
      exit(-1);
    }
    std::vector<std::string> expect{"(StudentRequest)", "(StudentResponse)"};
    if (result != expect) {
      fprintf(stderr,
              "ERROR Round %d ReplaceWith(rpc AddStudent(StudentRequest) returns (StudentResponse) {", rd);
      fprintf(stderr, "result unmatch\n");
      exit(-1);
    }
    rd++;
  }


  std::cout << "Passed ^_^" << std::endl;
}

int main(int argc, char** argv) {
  test_Strip();
  test_StartWith();
  test_EndWith();
  test_Split();
  test_ReplaceWith();
  test_GetToken();
  return 0;
}
