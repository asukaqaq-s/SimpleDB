#ifndef VIEW_MANAGER_CC
#define VIEW_MANAGER_CC

#include "metadata/view_manager.h"

namespace SimpleDB {

ViewManager::ViewManager(bool IsNew, 
                         TableManager *table_mgr, 
                         Transaction *txn) 
            : table_mgr_(table_mgr){
    // if the database is new one, we should
    // create a view_catch table and store its 
    // informations in catalog table
    if (IsNew) {
        Schema view_table_schema;
        
        // update schema
        view_table_schema.AddStringField("view_name", 
                                         MAX_TABLE_NAME_LENGTH);
        view_table_schema.AddStringField("view_def",
                                         MAX_VIEW_LENGTH);
        
        // add it to catalog table
        table_mgr_->CreateTable("view_catch",
                                 view_table_schema,
                                 txn);
    }

    
}

void ViewManager::CreateView(const std::string &view_name, 
                             const std::string &view_def,
                             Transaction *txn) {
    // create a view and add it to the view_table
    Layout view_layout = table_mgr_->GetLayout("view_catch", txn);
    TableScan view_table_scan(txn, "view_catch", view_layout);

    view_table_scan.Insert();
    // --------------------------------
    // | view_name | view_definitions |
    // --------------------------------
    view_table_scan.SetString("view_name", view_name);
    view_table_scan.SetString("view_def", view_def);

    view_table_scan.Close();
}

std::string ViewManager::GetViewDef(const std::string &view_name,
                                    Transaction *txn) {
    Layout view_layout = table_mgr_->GetLayout("view_catch", txn);
    TableScan view_table_scan(txn, "view_catch", view_layout);
    std::string view_def;

    while (view_table_scan.Next()) {
        std::string now_view_name = view_table_scan.GetString("view_name");
        
        if (view_name == now_view_name) {
            view_def = view_table_scan.GetString("view_def");
            break;
        }
    }
    
    // we can not return immediatly after find the corresponding view
    // otheriwse, this block will be pinned by the txn until stop
    view_table_scan.Close();
    return view_def;
}

} // namespace SimpleDB

#endif