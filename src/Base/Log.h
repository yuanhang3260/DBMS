#ifndef BASE_LOG_
#define BASE_LOG_

#include <iostream>

void LogToINFO(const char* file, int line, const char* func,
               const char* error_msg, ...);
void LogToERROR(const char* file, int line, const char* func,
                const char* error_msg, ...);
void LogToFATAL(const char* file, int line, const char* func,
                const char* error_msg, ...);

#define LogINFO(error_msg, ...)  \
  LogToINFO(__FILE__, __LINE__, __FUNCTION__, error_msg, ## __VA_ARGS__);

#define LogERROR(error_msg, ...)  \
  LogToERROR(__FILE__, __LINE__, __FUNCTION__, error_msg, ## __VA_ARGS__);

#define LogFATAL(error_msg, ...)  \
  LogToFATAL(__FILE__, __LINE__, __FUNCTION__, error_msg, ## __VA_ARGS__);

#define CheckLogFATAL(condition, error_msg, ...)  \
  if (!(condition)) {  \
    LogToFATAL(__FILE__, __LINE__, __FUNCTION__, error_msg, ## __VA_ARGS__);  \
  }

void debug(int i);

#endif  /* BASE_LOG_ */
