#ifndef BASIC_UPDATE_PLANNER_H
#define BASIE_UPDATE_PLANNER_H

#include "plan/update_planner.h"
#include "metadata/metadata_manager.h"

namespace SimpleDB {

class BasicUpdatePlanner : public UpdatePlanner {

public:
    
    BasicUpdatePlanner(MetadataManager *mdm) : mdm_(mdm) {}

    
    int ExecuteInsert(InsertData *data, Transaction *txn) override;

    int ExecuteDelete(DeleteData *data, Transaction *txn) override;

    int ExecuteModify(ModifyData *data, Transaction *txn) override;

    int ExecuteCreateTable(CreateTableData *data, Transaction *txn) override;

    int ExecuteCreateView(CreateViewData *data, Transaction *txn) override;

    int ExecuteCreateIndex(CreateIndexData *data, Transaction *txn) override;

private:

    MetadataManager *mdm_;

};

} // namespace SimpleDB

#endif