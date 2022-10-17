#ifndef SIMPLEDB_CC
#define SIMPLEDB_CC

#include "server/simpledb.h"
#include "config/config.h"
#include "plan/better_query_planner.h"
#include "plan/basic_update_planner.h"

#include <unistd.h>


namespace SimpleDB {

int SimpleDB::BLOCK_SIZE = SIMPLEDB_BLOCK_SIZE;
int SimpleDB::BUFFER_POOL_SIZE = SIMPLEDB_BUFFER_POOL_SIZE;
std::string LOG_FILE = SIMPLEDB_LOG_FILE_NAME;

SimpleDB::SimpleDB(const std::string &dir_name, 
                   int block_size, 
                   int buffer_size) {
    char buf[100];
    BLOCK_SIZE = block_size;
    BUFFER_POOL_SIZE = BUFFER_POOL_SIZE;
    std::string cwd = getcwd(buf, 100);

    if (cwd.empty()) {
        SIMPLEDB_ASSERT(false, "getcwd failed");
    }
    
    cwd += "/simpledb";
    cwd += "/" + dir_name;
    fm_ = std::make_unique<FileManager>(cwd, BLOCK_SIZE);
    lm_ = std::make_unique<LogManager>(fm_.get(), LOG_FILE);
    bm_ = std::make_unique<BufferManager>(fm_.get(), lm_.get(), BUFFER_POOL_SIZE);
}

SimpleDB::SimpleDB(const std::string &dir_name) :
    SimpleDB(dir_name, BLOCK_SIZE, BUFFER_POOL_SIZE){
    auto txn = std::make_unique<Transaction>(fm_.get(), lm_.get(), bm_.get());
    bool is_new = fm_->IsNew();
    
    if (is_new) {
        std::cout << "creating new database" << std::endl;
    } else {
        std::cout << "recovering existing database" << std::endl;
        txn->Recovery();
    }

    mdm_ = std::make_unique<MetadataManager>(is_new, txn.get());
    auto qp = std::make_unique<BetterQueryPlanner>(mdm_.get());
    auto up = std::make_unique<BasicUpdatePlanner>(mdm_.get());
    plan_ = std::make_unique<Planner> (std::move(qp), std::move(up));

    txn->Commit();
}

std::unique_ptr<Transaction> SimpleDB::GetNewTxn() {
    auto txn_ = std::make_unique<Transaction>(fm_.get(), lm_.get(), bm_.get());
    return txn_;
}

MetadataManager& SimpleDB::GetMetaDataMGR() {
    return *mdm_;
}

Planner& SimpleDB::GetPlanner() {
    return *plan_;
}

FileManager& SimpleDB::GetFileMGR() {
    return *fm_;
}

LogManager& SimpleDB::GetLogMGR() {
    return *lm_;
}

BufferManager& SimpleDB::GetBufferMGR() {
    return *bm_;
}


} // namespace SimpleDB


#endif