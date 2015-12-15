#include "stdlib.h"
#include "stdio.h"
#include "stdarg.h"

#include "Log.h"

void LogINFO(const char* error_msg, ...) {
  va_list args;
  va_start(args, error_msg);
  vfprintf(stdout, error_msg, args);
  va_end(args);
  fprintf(stdout, ".\n");
}

void LogERROR(const char* error_msg, ...) {
  va_list args;
  va_start(args, error_msg);
  vfprintf(stderr, error_msg, args);
  va_end(args);
  fprintf(stderr, ".\n");
}

void debug(int i) {
  std::cout << "debug " << i << std::endl;
}
