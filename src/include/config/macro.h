#ifndef MACRO_H
#define MACRO_H

#include <assert.h>

namespace SimpleDB {

#define SIMPLEDB_ASSERT(expr, message) assert((expr) && (message))

#define DEBUG_POINT() { std::cout << "1" << std::endl; exit(0); } 

#define LOG_INFO(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_INFO); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                               \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                        \
  ::fflush(stdout)


#define LOG_DEBUG(...)                                                      \
  OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                         \
  ::fflush(stdout)

}// namespace SimpleDB

#endif