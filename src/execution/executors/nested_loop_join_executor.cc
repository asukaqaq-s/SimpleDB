#ifndef NESTED_LOOP_JOIN_EXECUTOR_CC
#define NESTED_LOOP_JOIN_EXECUTOR_CC

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace SimpleDB {

void NestedLoopJoinExecutor::Init() {
    left_child_->Init();
    right_child_->Init();

    left_tuple_ = std::make_unique<Tuple> ();
    // move to the first tuple of left_child
    left_child_->Next(left_tuple_.get());

    for (const auto &t:node_->GetSchema()->GetColumns()) {
        value_expressions_.emplace_back(
            std::make_unique<ColumnValueExpression>(t.GetType(), t.GetName()));
    }
}



bool NestedLoopJoinExecutor::Next(Tuple *tuple) {
    SIMPLEDB_ASSERT(left_tuple_, "nullptr");
    Tuple right_tuple;
    
    auto node = GetPlanNode<NestedLoopJoinPlan>();

    while (right_child_->Next(&right_tuple)) {
        if (node.predicate_ != nullptr &&
            node.predicate_->EvaluateJoin(left_tuple_.get(), 
                                          left_child_->GetOutputSchema(),
                                          &right_tuple,
                                          right_child_->GetOutputSchema()).IsFalse()) {
            continue;
        }

        // success, generate a output tuple and return true
        *tuple = GenerateOutputTuple(&right_tuple);
        return true; 
    }

    right_child_->Init();

    while (left_child_->Next(left_tuple_.get())) {
        while (right_child_->Next(&right_tuple)) {
            
            if (node.predicate_ != nullptr &&
                node.predicate_->EvaluateJoin(left_tuple_.get(), 
                                              left_child_->GetOutputSchema(),
                                              &right_tuple,
                                              right_child_->GetOutputSchema()).IsFalse()) {
                continue;
            }

            // success, generate a ouput tuple and return true
            *tuple = GenerateOutputTuple(&right_tuple);
            return true; 
        }
        right_child_->Init();
    }

    return false;
}

Tuple NestedLoopJoinExecutor::GenerateOutputTuple(Tuple *right_tuple) {
    std::vector<Value> value_list;

    for (auto &t:value_expressions_) {
        value_list.emplace_back(t->EvaluateJoin(left_tuple_.get(), 
                                                left_child_->GetOutputSchema(),
                                                right_tuple,
                                                right_child_->GetOutputSchema()));
    }

    return Tuple(value_list, *node_->GetSchema());
}



} // namespace SimpleDB

#endif