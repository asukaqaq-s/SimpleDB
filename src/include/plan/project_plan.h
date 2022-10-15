#ifndef PROJECT_PLAN_H
#define PROJECT_PLAN_H

#include "plan/plan.h"
#include "query/scan.h"
#include "query/project_scan.h"

namespace SimpleDB {

class ProjectPlan : public Plan {

public:

    ProjectPlan(const std::shared_ptr<Plan> &plan,
                const std::vector<std::string> &fields)
        : plan_(plan) {
        for (auto t:fields) {
            sch_.AddSameField(t, plan_->GetSchema());
        }
    }
    
    /**
    * @brief creates a project scan for this query.
    */
    std::shared_ptr<Scan> Open() override {
        auto s = plan_->Open();
        return std::make_shared<ProjectScan>(s, sch_.GetFields());
    }

    /**
    * @brief Estimates the number of block accesses in 
    * the projection, which is the same as in the 
    * underlying query.
    */
    int GetAccessBlocks() override {
        return plan_->GetAccessBlocks();
    }

    /**
    * @brief  Estimates the number of output records in 
    * the projection, which is the same as in the 
    * underlying query.
    */
    int GetOutputTuples() override {
        return plan_->GetOutputTuples();
    }

    /**
    * @brief Estimates the number of distinct field values
    * in the projection, which is the same as in the 
    * underlying query.
    */
    int GetDistinctVals(const std::string &field_name) override {
        return plan_->GetDistinctVals(field_name);
    }

    Schema GetSchema() override {
        return sch_;
    }

private:

    std::shared_ptr<Plan> plan_;

    Schema sch_;

};

} // namespace SimpleDB

#endif
