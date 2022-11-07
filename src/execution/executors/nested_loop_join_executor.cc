#ifndef NESTED_LOOP_JOIN_EXECUTOR_CC
#define NESTED_LOOP_JOIN_EXECUTOR_CC

#include "execution/executors/nested_loop_join_executor.h"

namespace SimpleDB {

void NestedLoopJoinExecutor::Init() {
    left_child_->Init();
    right_child_->Init();
}



bool NestedLoopJoinExecutor::Next(Tuple *tuple) {
    Tuple outer_tuple;
    Tuple inner_tuple;

    auto node = GetPlanNode<NestedLoopJoinPlan>();
    
    // Time complexity is n^2 
    while (left_child_->Next(&outer_tuple)) {
        while (right_child_->Next(&inner_tuple)) {

            // if predicate is nullptr, we just return this tuple
            // and if statisfy conditions, return this tuple to father node  
            if (node.predicate_ != nullptr && 
                node.predicate_->Evaluate(&outer_tuple, &inner_tuple).IsFalse()) {
                continue;
            }

            Tuple tmp_tuple;
            std::vector<Value> value_list;
            int column_count = node.schema_->GetColumnsCount();

            SIMPLEDB_ASSERT(column_count == static_cast<int>(node.value_expressions_.size()), 
                            "size not match");

            // update tmp_tuple, only need to update the column 
            // which exists in output schema
            for (int i = 0;i < column_count;i ++) {
                // left and right children may have columns with duplicate names
                // so read which children is depend on user.
                value_list.emplace_back(node.value_expressions_[i]
                                        ->Evaluate(&outer_tuple, &inner_tuple));
            }

            // generate a tmp tuple
            // we don't need to set rid, because this tuple not exists in disk
            *tuple = Tuple(value_list, *node.schema_); 

            return true;
        }

        // let right child points to the first record
        right_child_->Init();
    }

    return false;
}



} // namespace SimpleDB

#endif