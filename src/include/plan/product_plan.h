#ifndef PRODUCT_PLAN_H
#define PRODUCT_PLAN_H

#include "plan/plan.h"
#include "query/product_scan.h"

namespace SimpleDB {

/**
* @brief the plan class corresponding the productu
* relational algebra operator.
*/
class ProductPlan : public Plan {

public:

    /**
    * @brief Creates a new product node in the query tree,
    * having the two specified subqueries.
    * @param p1 the left-hand subquery
    * @param p2 the right-hand subquery
    */
    ProductPlan(const std::shared_ptr<Plan> &p1,
                const std::shared_ptr<Plan> &p2)
        : plan1_(p1), plan2_(p2) {
            sch_.AddAllField(plan1_->GetSchema());
            sch_.AddAllField(plan2_->GetSchema());
    }

    /**
    * @brief creates a product scan for this query
    */
    std::shared_ptr<Scan> Open() override {
        auto scan1 = plan1_->Open();
        auto scan2 = plan2_->Open();
        
        return std::make_shared<ProductScan> (scan1, scan2);
    }

    /**
    * @brief Estimates the number of block accesses in 
    * the product.
    * The formula is:
    * B(product(p1,p2)) = B(p1) + R(p1)*B(p2)
    */
    int GetAccessBlocks() override {
        return plan1_->GetAccessBlocks() + 
               plan1_->GetOutputTuples() * 
               plan2_->GetAccessBlocks();
    }

    /**
    * @brief Estimates the number of output records in 
    * the product.
    * The formula is:
    * R(product(p1,p2)) = R(p1)*R(p2) 
    */
    int GetOutputTuples() override {
        return plan1_->GetOutputTuples() * 
               plan2_->GetOutputTuples();
    }

    /**
    * @brief Estimates the distinct number of field values 
    * in the product.Since the product does not increase or 
    * decrease field values, the estimate is the same as in 
    * the appropriate underlying query.
    */
    int GetDistinctVals(const std::string &field_name) override {
        if (plan1_->GetSchema().HasField(field_name)) {
            return plan1_->GetDistinctVals(field_name);
        } else {
            return plan2_->GetDistinctVals(field_name);
        }
    }

    /**
    * @brief returns the schema of the product
    */
    Schema GetSchema() override {
        return sch_;
    }

private:

    std::shared_ptr<Plan> plan1_, plan2_;

    Schema sch_;

};

} // namespace SimpleDB

#endif