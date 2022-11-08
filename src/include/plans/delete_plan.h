#ifndef DELETE_PLAN_H
#define DELETE_PLAN_H

#include "plans/abstract_plan.h"
#include "record/schema.h"

namespace SimpleDB {

/**
* @brief 
* DeletePlan, it will delete the specified tuple from child executor
*/
class DeletePlan : public AbstractPlan {
    
friend class DeleteExecutor;

public:
    
    /**
    * @brief Construct a new Delete Plan object.
    * 
    * @param child child plan to perform the scan
    * @param table_oid table oid
    */
    DeletePlan(AbstractPlan *child,
               const std::string &table_name)
        : AbstractPlan(PlanType::DeletePlan, nullptr, {child}),
          table_name_(table_name) {}
    

private:

    std::string table_name_;
};

} // namespace SimpleDB

#endif