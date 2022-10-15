#ifndef PLANNER_H
#define PLANNER_H

#include "plan/update_planner.h"
#include "plan/query_planner.h"

namespace SimpleDB {

class Planner {

public:

    Planner(std::unique_ptr<QueryPlanner> query_planner,
            std::unique_ptr<UpdatePlanner> update_planner)
        : query_planner_(std::move(query_planner)),
          update_planner_(std::move(update_planner)) {}

    
    /**
    * @brief Creates a plan for an SQL select statement, 
    * using the supplied planner.
    * @param qry the sql query string
    * @param txn
    */
    std::shared_ptr<Plan> CreateQueryPlan(const std::string &qry,
                                          Transaction *txn);

    /**
    * @brief Executes an SQL insert, delete, modify, 
    * or create statement.
    * The method dispatches to the appropriate method of the
    * supplied update planner,
    * depending on what the parser returns.
    * @param cmd the sql update string
    * @param tx the transaction
    */
    int ExecuteUpdate(const std::string &cmd,
                      Transaction *txn);

private:

    void VerifyQuery(QueryData *data) {}

    void VerifyUpdate(Object *data) {}

    
private:

    std::unique_ptr<QueryPlanner> query_planner_;
    
    std::unique_ptr<UpdatePlanner> update_planner_;
    
};

} // namespace SimpleDB

#endif