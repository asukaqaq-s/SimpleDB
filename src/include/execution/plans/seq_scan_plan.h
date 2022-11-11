#ifndef SEQ_SCAN_PLAN_H
#define SEQ_SCAN_PLAN_H

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {



/**
 * @brief 
 * SeqScan plan. we perform the seqential scan on a table with an optional predicate.
 * it combines with select op and project op in relational algebra.
 */
class SeqScanPlan : public AbstractPlan {

friend class SeqScanExecutor;

public:
    

    /**
    * @brief Construct a new Seq Scan Plan object.
    * Since SeqScanPlan must be the leaf node, we don't need to provide child nodes here.
    * 
    * @param schema Output Schema which consists of the columns of current table
    * @param predicate Optional Preicate
    * @param table_name Oid of table that we scanned
    */
    SeqScanPlan(SchemaRef schema, AbstractExpression *predicate, const std::string &table_name)
        : AbstractPlan(PlanType::SeqScanPlan, schema, {}),
          predicate_(predicate),
          table_name_(table_name) {}
        
    AbstractExpression *GetPredicate() const {
        return predicate_;
    }

    std::string GetTableName() const {
        return table_name_;
    }
          
private:

    // a expression tree
    AbstractExpression *predicate_;
    
    std::string table_name_;
};

} // namespace SimpleDB

#endif