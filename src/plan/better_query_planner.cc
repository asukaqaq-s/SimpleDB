#ifndef BETTER_QUERY_PLANNER_CC
#define BETTER_QUERY_PLANNER_CC

#include "plan/better_query_planner.h"
#include "plan/table_plan.h"
#include "plan/select_plan.h"
#include "plan/product_plan.h"
#include "plan/project_plan.h"
#include "parse/parser.h"

#include <iostream>

namespace SimpleDB {

std::shared_ptr<Plan> BetterQueryPlanner::CreatePlan
(QueryData *data, Transaction *txn) {
    std::vector<std::shared_ptr<Plan>> plans;
    
    // step 1: create a plan for each mentioned table or view
    for (auto &s : data->GetTables()) {
        std::string view_def = mdm_->GetViewDef(s, txn);
        
        if (view_def.size()) {
            // is a view, recursively plan the view
            Parser p(view_def);
            auto view_data = p.ParseQuery();
            auto tmp_plan = CreatePlan(view_data.get(), txn);

            plans.emplace_back(tmp_plan);
        } else {
            // is a table name
            plans.emplace_back(
                std::make_shared<TablePlan>(txn, s, mdm_));
        }
    }

    // step 2: create the product of all table plans
    std::shared_ptr<Plan> plan = plans[0];
    for (int i = 1;i < static_cast<int>(plans.size());i ++) {
        auto next_plan = plans[i];

        // try two orderings and choose the one
        // having lowest cost
        std::shared_ptr<Plan> choice1 = 
            std::make_shared<ProductPlan>(next_plan, plan);
        std::shared_ptr<Plan> choice2 = 
            std::make_shared<ProductPlan>(plan, next_plan);
        
        if (choice1->GetAccessBlocks() < 
            choice2->GetAccessBlocks()) {
            plan = choice1;
        } else {
            plan = choice2;
        }
    }

    // basic
    // std::shared_ptr<Plan> p = plans[0];
    // for (int i = 1;i < static_cast<int>(plans.size());i ++) {
    //     p = std::static_pointer_cast<Plan> (
    //         std::make_shared<ProductPlan>(p, plans[i])
    //     );
    // }

    
    // step 3: add a selection plan for the predicate
    plan = std::static_pointer_cast<Plan>(
           std::make_shared<SelectPlan>(plan, data->GetPred()));

    // step 4: project on the field names
    plan = std::static_pointer_cast<Plan>(
           std::make_shared<ProjectPlan>(plan, data->GetFields()));
    
    return plan;
}

} // namespace SimpleDB

#endif
