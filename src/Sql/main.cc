#include <iostream>

#include "Query/Interpreter.h"

int main(int argc, char **argv) {
  // This evaluator only works for const values.
  Storage::DataRecord record;
  Query::EvaluateArgs evalute_args(nullptr, record, Storage::DATA_RECORD, {});

  Query::Interpreter i(nullptr);
  i.set_debug(true);

  bool success = false;
  if (argc > 1) {
    success = i.Parse(argv[1]);
  } else {
    success = i.Parse();
  }

  auto node = i.GetCurrentNode();
  if (node && !node->valid()) {
    LogERROR("Invalid node - %s", node->error_msg().c_str());
  } else {
    //auto value = node->Evaluate(evalute_args);
    //std::cout << value.AsString() << std::endl;
  }

  std::cout << "Parse "
            << (success && node && node->valid()? "success" : "failed")
            << std::endl;
  return 0;
}
