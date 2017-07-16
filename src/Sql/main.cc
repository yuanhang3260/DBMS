#include <iostream>

#include "Query/Interpreter.h"

using namespace Sql;

int main(int argc, char **argv) {
  Query::Interpreter i;
  i.set_debug(true);

  bool success = false;
  if (argc > 1) {
    success = i.parse(argv[1]);
  } else {
    success = i.parse();
  }

  auto node = i.GetCurrentNode();
  if (!node->valid()) {
    LogERROR("Invalid node - %s", node->error_msg().c_str());
  } else {
    auto value = node->Evaluate();
    std::cout << value.AsString() << std::endl;
  }

  std::cout << "Parse "
            << (success && node->valid()? "success" : "failed") << std::endl;
  return 0;
}
