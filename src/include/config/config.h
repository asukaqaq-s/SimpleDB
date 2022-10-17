#ifndef CONFIG_H
#define CONFIG_H

#include <string>

namespace SimpleDB {

static const std::string TABLE_CATCH = "table_catch";
static const std::string FIELD_CATCH = "field_catch";
static const std::string INDEX_CATCH = "index_catch";

static constexpr int SIMPLEDB_BLOCK_SIZE = 4096;
static constexpr int SIMPLEDB_BUFFER_POOL_SIZE = 100;
static const std::string SIMPLEDB_LOG_FILE_NAME = "simpledb.log";

} // namespace SimpleDB

#endif
