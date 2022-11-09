#ifndef UPDATE_EXECUTOR_CC
#define UPDATE_EXECUTOR_CC

#include "execution/executors/update_executor.h"

namespace SimpleDB {


// a simple SQL statement that implements update format
// 
// UPDATE table_name 
// SET (F1 = C1,F2 = C2, F3 = C3)...
// WHERE ...
// 
// in our implementation, we can generate the executor tree as following
// 
//                            UPDATE EXECUTOR
//                                  |
//                            a query executor tree
//

void UpdateExecutor::Init() {

    child_->Init();
    auto node = GetPlanNode<UpdatePlan>();
    
    txn_ = content_->GetTransactionContext();
    table_info_ = content_->GetMetadataMgr()->GetTable(node.table_name_, txn_);
    table_schema_ = &table_info_->schema_;

    // check return type
    for (auto &update_info : node.update_list_) {
        auto type = table_schema_->GetColumn(update_info.column_name_).GetType();

        if (type == TypeID::CHAR && 
            update_info.expression_->GetReturnType() == TypeID::VARCHAR) {
            
        }
        else {
            SIMPLEDB_ASSERT(type == update_info.expression_->GetReturnType(), 
                            "type is not match");
        } 
    }

    
}


bool UpdateExecutor::Next(Tuple *tuple) {
    
    auto *table_heap = table_info_->table_heap_.get();
    Tuple tmp;

    if (child_->Next(&tmp)) {
        
        // generate a new tuple
        Tuple new_tuple = GenerateUpdatedTuple(tmp);
        RID rid = new_tuple.GetRID();    

        // update in this loction
        table_heap->Update(txn_, rid, new_tuple);
        return true;
    }

    return false;
}


Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &tuple) {
    // maybe every columns not be updated at the same time
    // but a new tuple also need to store the old_value of columns
    // which not be updated
    auto node = GetPlanNode<UpdatePlan>();
    std::vector<Value> value_list;

    // insert all columns
    for (auto &t:table_schema_->GetColumns()) {
        value_list.emplace_back(Value(tuple.GetValue(t.GetName(), *table_schema_)));
    }

    // update columns
    for (auto &update_info:node.update_list_) {
        int index = table_schema_->GetColumnIdx(update_info.column_name_);
        assert(index >= 0 && index < static_cast<int>(value_list.size()));
        
        // We will only have one child at most
        // so it always exist in left child node
        value_list[index] = update_info.expression_->Evaluate(&tuple, *table_schema_);
    }
    
    // constructor new tuple
    Tuple new_tuple(value_list, *table_schema_);
    new_tuple.SetRID(tuple.GetRID());

    return new_tuple;
}



} // namespace SimpleDB


#endif