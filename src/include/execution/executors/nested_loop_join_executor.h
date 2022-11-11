#ifndef NESTED_LOOP_JOIN_EXECUTOR_H
#define NESTED_LOOP_JOIN_EXECUTOR_H

#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/expressions/column_value_expression.h"

#include <memory>

namespace SimpleDB {

/**
 * @brief 
 * Executor to execute nested loop join. Check NestedLoopJoinPlan for more details
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
public:
    NestedLoopJoinExecutor(ExecutionContext *context, AbstractPlan *node,
                           std::unique_ptr<AbstractExecutor> &&left_child,
                           std::unique_ptr<AbstractExecutor> &&right_child)
        : AbstractExecutor(context, node),
          left_child_(std::move(left_child)),
          right_child_(std::move(right_child)) {}
    
    void Init() override;

    bool Next(Tuple *tuple) override;

private:

    Tuple GenerateOutputTuple(Tuple *right_tuple);


    std::unique_ptr<AbstractExecutor> left_child_;
    std::unique_ptr<AbstractExecutor> right_child_;

    // cache left tuple
    std::unique_ptr<Tuple> left_tuple_;

    // value expression array which help us construct a new output_tuple
    // cache to avoid multipy penalty.
    std::vector<std::unique_ptr<ColumnValueExpression>> value_expressions_;
};



} // namespace SimpleDB

#endif