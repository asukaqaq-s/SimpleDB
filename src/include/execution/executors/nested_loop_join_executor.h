#ifndef NESTED_LOOP_JOIN_EXECUTOR_H
#define NESTED_LOOP_JOIN_EXECUTOR_H

#include "execution/executors/abstract_executor.h"
#include "plans/nested_loop_join_plan.h"

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

    std::unique_ptr<AbstractExecutor> left_child_;
    std::unique_ptr<AbstractExecutor> right_child_;
};



} // namespace SimpleDB

#endif