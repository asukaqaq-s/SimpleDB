#ifndef MACRO_H
#define MACRO_H

#include <assert.h>

namespace SimpleDB {

#define SIMPLEDB_ASSERT(expr, message) assert((expr) && (message))

#define DEBUG_POINT() { std::cout << "1" << std::endl; exit(0); } 


}// namespace SimpleDB

#endif