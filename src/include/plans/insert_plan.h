#ifndef INSERT_PLAN_H
#define INSERT_PLAN_H

#include "plans/abstract_plan.h"
#include "record/tuple.h"

namespace SimpleDB {

/**
 * @brief 
 * InsertPlan is used to perform the insertion to a table. The value could be
 * "Raw Value", i.e. user specify the exact value, or it could be the output from 
 * child executor.
 * 
 * For the "Raw Value", we will let the front-end to check the value integrity and 
 * construct the final tuple before entering InsertPlanNode
 * 
 * For the "child executor", we will call the "Next" method to find where position
 * we can insert.
 */
class InsertPlan : public AbstractPlan {

    // it's not necessary to write Getter function for private members
    friend class InsertExecutor;

public:


    /**
     * @brief Construct a new Insert Plan object.
     * This is the constructor for inserting raw values.
     * @param tuples 
     * @param table_oid 
     */
    InsertPlan(std::vector<Tuple> &&tuples, 
               const std::string &table_name)
        : AbstractPlan(PlanType::InsertPlan, nullptr, {}),
          tuples_(std::move(tuples)),
          table_name_(table_name) {}


    /**
     * @brief Construct a new Insert Plan object.
     * This is the constructor for inserting values that is the output from child executor.
     * @param child 
     * @param table_oid 
     */
    InsertPlan(AbstractPlan *child, const std::string table_name)
        : AbstractPlan(PlanType::InsertPlan, nullptr, {child}),
          table_name_(table_name) {}
    
    /**
     * @brief 
     * 
     * @return whether this insert plan is inserting raw value
     */
    bool IsRawInsert() {
        return GetChildren().empty();
    }

private:

    // raw value to be inserted
    std::vector<Tuple> tuples_;

    // table to be inserted to
    std::string table_name_;
};

}

#endif