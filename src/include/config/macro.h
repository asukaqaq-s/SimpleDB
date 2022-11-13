#ifndef MACRO_H
#define MACRO_H

#include <assert.h>

namespace SimpleDB {

#define SIMPLEDB_ASSERT(expr, message) assert((expr) && (message))




}// namespace SimpleDB

#endif