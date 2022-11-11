#ifndef UPDATE_PLAN_H
#define UPDATE_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"


namespace SimpleDB {

class UpdateInfo;

using UpdateInfoRef = std::shared_ptr<UpdateInfo>;


/**
* @brief a updateinfo object includes predicate 
*/
class UpdateInfo {

public:

    UpdateInfo(AbstractExpressionRef expression, const std::string &column_name)
        : expression_(expression), column_name_(column_name) {}

    
    // a update sql statement will like
    // UPDATE table_name SET field_name = constant
    // so expression's type always is constant_expression
    AbstractExpressionRef expression_{nullptr};

    // column_name for table schema.
    std::string column_name_;
};


/**
 * @brief 
 * Update plan node. it will read tuple from child executor, then perform the updation.
 * Update operation could either be a Set, which we need to specify the exact value to be set,
 * or it could be a Operation, e.g. a = a + 1, which we need to specify the OperationExpression.
 * such as, UPDATE table_name SET field_A = 'ca', field_B = 123, field_C = field_C + 2
 */
class UpdatePlan : public AbstractPlan {

    friend class UpdateExecutor;

public:

    /**
     * @brief Construct a new Update Plan object.
     * 
     * @param node child node
     * @param table_oid table oid to perform updation
     * @param update_list list that contains the information to perform updation
     */
    UpdatePlan(AbstractPlan *node, 
               const std::string &table_name, 
               std::vector<UpdateInfo> &&update_list)
        : AbstractPlan(PlanType::UpdatePlan, nullptr, {node}),
          table_name_(table_name),
          update_list_(std::move(update_list)) {}
    

private:

    std::string table_name_;
    
    std::vector<UpdateInfo> update_list_;
};



} // namespace SimpleDB


#endif