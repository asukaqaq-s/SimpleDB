#ifndef EXECUTOR_CONTEXT_H
#define EXECUTOR_CONTEXT_H

#include "concurrency/transaction.h"
#include "record/table_heap.h"
#include "metadata/metadata_manager.h"

namespace SimpleDB {

/**
* @brief
*/
class ExecutionContext {

public:

    ExecutionContext(MetadataManager *mdm, BufferManager *bfm, 
                     Transaction *txn, TransactionManager *txn_mgr)
                : metadata_(mdm), bfm_(bfm), txn_(txn), txn_mgr_(txn_mgr) {
        SIMPLEDB_ASSERT(txn_mgr_ == txn->GetTxnManager(), "");
    }

    
    ~ExecutionContext() = default;
    
    inline MetadataManager *GetMetadataMgr() {
        return metadata_;
    }

    inline BufferManager *GetBufferPoolManager() {
        return bfm_;
    }

    inline TransactionManager *GetTransactionManager() {
        return txn_mgr_;
    }

    inline Transaction *GetTransactionContext() {
        return txn_;
    }

    void SetTransactionManager(TransactionManager *txn_manager) {
        txn_mgr_ = txn_manager;
    }

    void SetTransaction(Transaction *txn) {
        txn_ = txn;
    }

private:

    MetadataManager *metadata_;

    BufferManager *bfm_;

    Transaction *txn_;

    TransactionManager *txn_mgr_{nullptr};

};

} // namespace SimpleDB

#endif
