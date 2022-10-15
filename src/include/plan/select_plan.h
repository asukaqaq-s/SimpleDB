#ifndef SELECT_PLAN_H
#define SELECT_PLAN_H

#include "plan/plan.h"
#include "query/select_scan.h"
#include "query/predicate.h"

namespace SimpleDB {

class SelectPlan : public Plan {

public:

    SelectPlan(const std::shared_ptr<Plan> &plan, 
               const Predicate &pred) :
        plan_(plan), pred_(pred) {}
    
    /**
    * @brief Creates a select scan for this query.
    * @see simpledb.plan.Plan#open()
    */
    std::shared_ptr<Scan> Open() override {
        auto scan = plan_->Open();
        return std::make_shared<SelectScan>(scan, pred_);
    }

    /**
    * @brief the number of block accesses in the selection,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
    int GetAccessBlocks() override {
        // all block all should be accessed
        return plan_->GetAccessBlocks();
    }

    /**
    * @brief Estimates the number of output records 
    * in the selection
    */
    int GetOutputTuples() override {
        return plan_->GetOutputTuples() /
               pred_.reductionFactor(plan_.get());
    }

    /**
    * @brief Estimates the number of distinct field values
    * in the projection.
    */
    int GetDistinctVals(const std::string &field_name) override {
        // If the predicate contains a term equating the specified 
        // field to a constant, then this value will be 1.
        // Otherwise, it will be the number of the distinct values
        // in the underlying query 
        // (but not more than the size of the output table).
        
        if (pred_.EquatesWithConsant(field_name).IsNull()) {
            std::string field_name_tmp = pred_.EquatesWithField(field_name);
            
            // if the predicate have a term is "F1=F2" which
            // F1 is the sepcified fieldname, and we can find F2
            // distinct values is determined by min(F1,F2)
            if (field_name_tmp.size()) {
                return std::min(plan_->GetDistinctVals(field_name),
                                plan_->GetDistinctVals(field_name_tmp));
            }
            else {
                // if can not find this term
                // distinct values is determined by F
                return plan_->GetDistinctVals(field_name);
            }
        }
        else {
            // if the predicate have a term is "F=c" which
            // F is the specified field name
            // then, the field only have one values after filter
            return 1;
        }
    }
    
    /**
    * @brief returns the schema of the selection
    */
    Schema GetSchema() override {
        return plan_->GetSchema();
    }

private:

    std::shared_ptr<Plan> plan_;
    
    Predicate pred_;
};

} // namespace SimpleDB

#endif