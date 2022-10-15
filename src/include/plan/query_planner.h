#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include "plan/plan.h"
#include "parse/query_data.h"
#include "concurrency/transaction.h"

namespace SimpleDB {

class QueryPlanner {

public:

    virtual ~QueryPlanner() = default;

    virtual std::shared_ptr<Plan> CreatePlan(QueryData *data, Transaction *txn) = 0;

};

} // namespace SimpleDB

#endif