#ifndef SIMPLEDB_H
#define SIMPLEDB_H

#include "plan/planner.h"
#include "concurrency/transaction.h"
#include "buffer/buffer_manager.h"
#include "file/file_manager.h"
#include "log/log_manager.h"
#include "metadata/metadata_manager.h"

#include <memory>

namespace SimpleDB {

class SimpleDB {

public:

    static  int BLOCK_SIZE;
    static  int BUFFER_POOL_SIZE;
    static  std::string LOG_FILE_NAME;

    SimpleDB(const std::string &dir_name,
             int block_size,
             int buffer_pool_size);
    
    SimpleDB(const std::string &dir_name);

    std::unique_ptr<Transaction> GetNewTxn();

    MetadataManager &GetMetaDataMGR();

    Planner &GetPlanner();

    FileManager &GetFileMGR();

    LogManager &GetLogMGR();

    BufferManager &GetBufferMGR();
    

private:

    std::unique_ptr<FileManager> fm_;
    
    std::unique_ptr<BufferManager> bm_;

    std::unique_ptr<Planner> plan_;
    
    std::unique_ptr<LogManager> lm_;
    
    std::unique_ptr<MetadataManager> mdm_;
};

} // namespace SimpleDB

#endif