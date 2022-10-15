#ifndef BETTER_QUERY_PLANNER_H
#define BETTER_QUERY_PLANNER_H

#include "plan/query_planner.h"
#include "metadata/metadata_manager.h"


namespace SimpleDB {

class BetterQueryPlanner : public QueryPlanner {

public:

    BetterQueryPlanner(MetadataManager *mdm) : mdm_(mdm) {}

    /**
    * @brief Creates a query plan as follows.  
    * It first takes the product of all tables and views; 
    * it then selects on the predicate;
    * and finally it projects on the field list. 
    * @param data
    * @param txn
    */
    std::shared_ptr<Plan> CreatePlan(QueryData *data, Transaction *txn) override;


private:

    MetadataManager *mdm_;

};

} // namespace SimpleDB

#endif
