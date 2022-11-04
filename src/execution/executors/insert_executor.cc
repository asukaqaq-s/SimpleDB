#ifndef INSERT_EXECUTOR_CC
#define INSERT_EXECUTOR_CC

#include "execution/executors/insert_executor.h"

namespace SimpleDB {

// a simple SQL statement that implements insertion format
// 
// RAW INSERT: INSERT INTO table_name (column_list) VALUES (values_list)
//
// INSERT FROM SELECT: INSERT INTO table_name SELECT ... FROM table_name2
// 
// in our implementation, we can generate the executor tree as following
//
// RAW INSERT:   INSERT EXECUTOR
//                     |
//               constant-expression
//
// INSERT FROM SELECT:  INSERT EXECUTOR
//                            |
//                      a query executor tree
//


void InsertExecutor::Init() {
    auto node = GetPlanNode<InsertPlan>();
    
    // has children
    if (!node.IsRawInsert()) {
        child_->Init();
    }

    txn_ = content_->GetTransactionContext();
    table_info_ = content_->GetMetadataMgr()->GetTable(node.table_name_, txn_);
    table_schema_ = &table_info_->schema_;
}


bool InsertExecutor::Next(Tuple *tuple) {
    auto node = GetPlanNode<InsertPlan>();
    
    // update and delete maybe has a limit_executor as his father node
    // so we can't use while loop in its next method
    // however, for insertion, it don't have any father node.
    if (node.IsRawInsert()) {
        for (const auto &tuple : node.tuples_) {
            RID rid;
            table_info_->table_heap_->Insert(txn_, tuple, &rid);
        }
    }
    else {
        Tuple tmp;
        RID rid;
        SIMPLEDB_ASSERT(child_ != nullptr, "");

        while (child_->Next(&tmp)) {
            table_info_->table_heap_->Insert(txn_, tmp, &rid);
        }
    }

    // for insertion, we just insert once so don't need to return true
    return false;
}


} // namespace SimpleDB

#endif
