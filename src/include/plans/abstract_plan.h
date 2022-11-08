#ifndef ABSTRACT_PLAN_H
#define ABSTRACT_PLAN_H

#include <vector>
#include "record/schema.h"

namespace SimpleDB {

enum class PlanType {
    AbstractPlan, // invalid type
    // for querying
    SeqScanPlan,       
    NestedLoopJoinPlan, // corresponding to the simplest product operator
    // for updating  
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    // leaf node, basic plan type
    TablePlan,
};


/**
 * @brief 
 * Base abstract class for plan nodes.
 */
class AbstractPlan {


public:


using SchemaRef = std::shared_ptr<Schema>;


public:

    AbstractPlan(PlanType type, 
                 SchemaRef schema, 
                 std::vector<AbstractPlan *> &&children)
        : schema_(schema), children_(std::move(children)), type_(type) {}
    
    virtual ~AbstractPlan() = default;

    Schema GetSchema() const {
        return *schema_;
    }

    /**
    * @brief
    * Get the child node though idx
    * @param idx 
    * @return AbstractExpression* 
    */
    AbstractPlan *GetChildAt(int idx) const {
        return children_[idx];
    }

    std::vector<AbstractPlan *> GetChildren() const {
        return children_;
    }

    PlanType GetType() const {
        return type_;
    }


protected:
    
    // out put schema for current node
    // why we need schema instead of schema?
    // becaues 
    SchemaRef schema_;
    
    // children nodes
    std::vector<AbstractPlan *> children_;
    
    // plan type
    PlanType type_;
};



} // namespace SimpleDB

#endif