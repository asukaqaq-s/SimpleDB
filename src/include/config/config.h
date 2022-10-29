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
// store checkpoint_end_record's lsn
static const std::string SIMPLEDB_CHKPT_FILE_NAME = "checkpoint.log";


enum DeadLockResolveProtocol {
    DO_NOTHING,
    WOUND_WAIT,
    WAIT_DIE,
    DL_DETECT,
};

static const DeadLockResolveProtocol GLOBAL_DEAD_LOCK_DEAL_PROTOCOL = WOUND_WAIT;

} // namespace SimpleDB

#endif
