#ifndef PLANNER_CC
#define PLANNER_CC

#include "plan/planner.h"
#include "parse/parser.h"
#include "parse/object.h"

namespace SimpleDB {

std::shared_ptr<Plan> Planner::CreateQueryPlan
(const std::string &qry,Transaction *txn) {
    Parser parser(qry);
    auto data = parser.ParseQuery();
    VerifyQuery(data.get());
    
    return query_planner_->CreatePlan(data.get(), txn);
}

int Planner::ExecuteUpdate
(const std::string &cmd,Transaction *txn) {
    Parser parser(cmd);
    auto data = parser.ParseUpdateCmd();
    VerifyUpdate(data.get());
    
    auto op = data->GetOP();
    
    switch(op) {
    
    case Object::INSERT:
        return update_planner_->ExecuteInsert(
            static_cast<InsertData*>(data.get()), txn);
        break;
    
    case Object::REMOVE:
        return update_planner_->ExecuteDelete(
            static_cast<DeleteData*>(data.get()), txn);
        break;
    
    case Object::MODIFY:
        return update_planner_->ExecuteModify(
            static_cast<ModifyData*>(data.get()), txn);
        break;

    case Object::CREATETABLE:
        return update_planner_->ExecuteCreateTable(
            static_cast<CreateTableData*>(data.get()), txn);
        break;
    
    case Object::CREATEVIEW:
        return update_planner_->ExecuteCreateView(
            static_cast<CreateViewData*>(data.get()), txn);
        break;
    
    case Object::CREATEINDEX:
        return update_planner_->ExecuteCreateIndex(
            static_cast<CreateIndexData*>(data.get()), txn);
        break;
    }

    return 0;
}




} // namespace SimpleDB

#endif