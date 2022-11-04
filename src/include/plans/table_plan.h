#ifndef TABLE_PLAN_H
#define TABLE_PLAN_H

#include "plans/abstract_plan.h"

namespace SimpleDB {

/**
* @brief table_plan is the most basic plan class, 
* and is the leaf nodes of the query tree
*/
class TablePlan : public AbstractPlan {

public:

    TablePlan(Schema *schema, const std::string &table_name)
    : AbstractPlan(PlanType::TablePlan, schema, {}), table_name_(table_name) {}

    

private:

    std::string table_name_;
    

};


} // namespace SimpleDB

#endif