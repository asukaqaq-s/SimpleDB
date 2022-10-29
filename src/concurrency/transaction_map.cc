#ifndef TRANSACTION_MAP_CC
#define TRANSACTION_MAP_CC

#include "concurrency/transaction.h"

namespace SimpleDB {

txn_id_t Transaction::NextTxnID() { 
    int res = txn_map.GetNextTransactionID();
    return res;
}

TransactionMap::TransactionMap() {}
    
TransactionMap::~TransactionMap() {}

void TransactionMap::InsertTransaction(Transaction *txn) {
    std::lock_guard<std::mutex> latch(latch_);
    txn_map_[txn->GetTxnID()] = txn;
}

void TransactionMap::RemoveTransaction(Transaction *txn) {
    std::lock_guard<std::mutex> latch(latch_);
    txn_map_.erase(txn->GetTxnID());
}

bool TransactionMap::IsTransactionAlive(txn_id_t txn_id) {
    std::lock_guard<std::mutex> latch(latch_);
    return txn_map_.find(txn_id) != txn_map_.end();            
}

Transaction* TransactionMap::GetTransaction(txn_id_t txn_id) {
    std::lock_guard<std::mutex> latch(latch_);

    SIMPLEDB_ASSERT(txn_map_.find(txn_id) != txn_map_.end(), "");
    return txn_map_[txn_id];
}

txn_id_t TransactionMap::GetNextTransactionID() {
    std::lock_guard<std::mutex> latch(latch_);
    int res = ++next_txn_id_;
    return res;
}

} // namespace SimpleDB


#endif