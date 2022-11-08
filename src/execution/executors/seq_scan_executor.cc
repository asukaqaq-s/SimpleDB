#ifndef SEQ_SCAN_EXECUTOR_CC
#define SEQ_SCAN_EXECUTOR_CC

#include "execution/executors/seq_scan_executor.h"

namespace SimpleDB {


void SeqScanExecutor::Init() {
    auto node = GetPlanNode<SeqScanPlan>();
    txn_ = content_->GetTransactionContext();
    table_info_ = content_->GetMetadataMgr()->GetTable(node.table_name_, txn_);
    
    table_schema_ = &table_info_->schema_;
    iterator_ = table_info_->table_heap_->Begin(txn_);
}


bool SeqScanExecutor::Next(Tuple *tuple) {
    auto node = GetPlanNode<SeqScanPlan>();
    while (!iterator_.IsEnd()) {
        Tuple tmp_tuple = iterator_.Get();
        iterator_++;
        
        // this tuple is not statify request
        // skip it and move to the next tuple
        if (node.predicate_ && 
            !node.predicate_->Evaluate(&tmp_tuple, *table_schema_).IsTrue()) {
            continue;
        }
        
        *tuple = tmp_tuple.KeyFromTuple(table_schema_, node.schema_.get());
        return true;
    }

    return false;
}

// nested join
// 


} // namespace SimpleDB

#endif