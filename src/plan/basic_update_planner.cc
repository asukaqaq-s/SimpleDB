#ifndef BASIC_UPDATE_PLANNER_CC
#define BASIC_UPDATE_PLANNER_CC


#include "plan/basic_update_planner.h"
#include "plan/table_plan.h"
#include "plan/select_plan.h"

namespace SimpleDB {

int BasicUpdatePlanner::ExecuteInsert
(InsertData *data, Transaction *txn) {
    std::shared_ptr<Plan> p = 
        std::make_shared<TablePlan>(txn, data->GetTableName(), mdm_);
    
    std::shared_ptr<UpdateScan> update = 
        std::static_pointer_cast<UpdateScan>(p->Open());
    
    update->Insert();
    std::vector<Constant> q = data->GetVals();
    std::vector<std::string> f = data->GetFields();
    
    for (int i = 0;i < static_cast<int>(q.size());i ++) {
        update->SetVal(f[i], q[i]);
    }
    

    update->Close();
    return true;
}

int BasicUpdatePlanner::ExecuteDelete
(DeleteData *data, Transaction *txn) {
    std::shared_ptr<Plan> p = 
        std::make_shared<TablePlan>(txn, data->GetTableName(), mdm_);
    p = std::make_shared<SelectPlan>(p, data->GetPred());
    
    std::shared_ptr<UpdateScan> update = 
        std::static_pointer_cast<UpdateScan>(p->Open());
    
    int cnt = 0;
    while (update->Next()) {
        update->Remove();
        cnt ++;
    }
    update->Close();
    return cnt;
}

int BasicUpdatePlanner::ExecuteModify
(ModifyData *data, Transaction *txn) {

    std::shared_ptr<Plan> p = 
        std::make_shared<TablePlan>(txn, data->GetTableName(), mdm_);
    std::shared_ptr<UpdateScan> update = 
        std::static_pointer_cast<UpdateScan>(p->Open());
    
    int cnt = 0;
    while (update->Next()) {
        Constant val = data->GetNewValue().Evaluate(update.get());
        update->SetVal(data->GetFieldNmae(), val);
        cnt ++;
    }

    update->Close();
    return cnt;
}

int BasicUpdatePlanner::ExecuteCreateTable
(CreateTableData *data, Transaction *txn) {
    std::cout << "table name = " << data->GetTableName() << std::endl;
    mdm_->CreateTable(data->GetTableName(), data->GetSchema(), txn);
    return 0;
}


int BasicUpdatePlanner::ExecuteCreateView
(CreateViewData *data, Transaction *txn) {
    mdm_->CreateView(data->GetViewName(), data->GetViewDef(), txn);
    return 0;
}

int BasicUpdatePlanner::ExecuteCreateIndex
(CreateIndexData *data, Transaction *txn) {
    mdm_->CreateIndex(data->GetIndexName(), data->GetTableName(),
                      data->GetFieldName(), txn);
    return 0;
}




}

#endif