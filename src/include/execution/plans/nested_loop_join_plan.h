#ifndef NESTED_LOOP_JOIN_PLAN_H
#define NESTED_LOOP_JOIN_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {


/**
* @brief 
* this class corresponding to product and project operator in relational algebra but 
* it's implemented in the simplest way.Because project operations is easy, So we do a 
* project by the way when we join or select.
*
* Plan used to perform nested loop join. Requires 2 child plan to generate tuple 
* and a predicate to evaluate the legality. We will simply use the cartesian product 
* to generate tuples then use a predicate to filter them.
*/
class NestedLoopJoinPlan : public AbstractPlan {

    friend class NestedLoopJoinExecutor;

public:

    /**
    * @brief Construct a new Nested Loop Join Plan object.
    * 
    * @param schema output schema which maybe consists of the columns of two children
    * @param children children plan
    * @param predicate predicate used to evaluate the legality of output tuple.
    * @param value_expressions expressions used to generate value for new tuple
    */
    NestedLoopJoinPlan(SchemaRef schema, std::vector<AbstractPlan *> &&children, 
                       AbstractExpression *predicate)
        : AbstractPlan(PlanType::NestedLoopJoinPlan, schema, std::move(children)),
          predicate_(predicate) {
        
        if (predicate_) {
            SIMPLEDB_ASSERT(predicate_->GetReturnType() == TypeID::INTEGER, "Invalid predicate");
        }
        SIMPLEDB_ASSERT(children_.size() == 2, "Wrong children size");
    }

    NestedLoopJoinPlan(SchemaRef schema, AbstractPlan *left, AbstractPlan *right, 
                       AbstractExpression *predicate)
        : AbstractPlan(PlanType::NestedLoopJoinPlan, schema, {left, right}),
          predicate_(predicate) {
        if (predicate_) {
            SIMPLEDB_ASSERT(predicate_->GetReturnType() == TypeID::INTEGER, "Invalid predicate");
        }
        SIMPLEDB_ASSERT(children_.size() == 2, "Wrong children size");
    }

    

private:

    // predicate used to evaluate the legality
    AbstractExpression *predicate_;
};

} // namespace SimpleDB

#endif