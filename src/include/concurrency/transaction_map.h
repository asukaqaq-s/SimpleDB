#ifndef TRANSACTION_MAP_H
#define TRANSACTION_MAP_H

#include "concurrency/transaction.h"

namespace SimpleDB {

class TransactionMap {

public:

    TransactionMap();
    
    ~TransactionMap();
    
    void InsertTransaction(Transaction *txn) {
        std::lock_guard<std::mutex> latch(latch_);
        txn_map_[txn->GetTxnID()] = txn;
    }

    void RemoveTransaction(Transaction *txn) {
        std::lock_guard<std::mutex> latch(latch_);
        txn_map_.erase(txn->GetTxnID());
    }

    bool IsTransactionAlive(txn_id_t txn_id) {
        std::lock_guard<std::mutex> latch(latch_);
        return txn_map_.find(txn_id) == txn_map_.end();
    }

    Transaction* GetTransaction(txn_id_t txn_id) {
        std::lock_guard<std::mutex> latch(latch_);
        if (txn_map_.find(txn_id) == txn_map_.end()) {
            return nullptr;
        }
        return txn_map_[txn_id];
    }

    txn_id_t GetNextTransactionID() {
        std::lock_guard<std::mutex> latch(latch_);
        return next_txn_id_++;
    }
    
private:

    std::unordered_map<txn_id_t, Transaction*> txn_map_;

    std::mutex latch_;

    txn_id_t next_txn_id_{0};
};


} // namespace SimpleDB

#endif