#ifndef VIEW_MANAGER_CC
#define VIEW_MANAGER_CC

#include "metadata/view_manager.h"

namespace SimpleDB {

ViewManager::ViewManager(bool IsNew, 
                         TableManager *table_mgr, 
                         Transaction *txn) 
            : table_mgr_(table_mgr){

    Schema view_table_schema;
        
    // update schema
    view_table_schema.AddColumn(Column(VIEW_TABLE_NAME_FIELD,
                                       TypeID::VARCHAR, 
                                       MAX_TABLE_NAME_LENGTH));
    view_table_schema.AddColumn(Column(VIEW_VIEW_DEF_FIELD,
                                       TypeID::VARCHAR,
                                       MAX_VIEW_LENGTH));     

    // if the database is new one, we should
    // create a view_catch table and store its 
    // informations in catalog table
    if (IsNew) {
        // add it to catalog table
        table_mgr_->CreateTable(VIEW_TABLE_NAME,
                                view_table_schema,
                                txn);
    }

    table_info_ = table_mgr_->GetTable(VIEW_TABLE_NAME,txn);
}

void ViewManager::CreateView(const std::string &view_name, 
                             const std::string &view_def,
                             Transaction *txn) {
    // create a view and add it to the view_table
    auto *table_heap = table_info_->table_heap_.get();
    auto schema = table_info_->schema_;

    // --------------------------------
    // | view_name | view_definitions |
    // --------------------------------
    std::vector<Value> field_list{ 
        Value(view_name, TypeID::VARCHAR), 
        Value(view_def, TypeID::VARCHAR)
    };
    Tuple inserted_tuple(field_list, schema);
    RID rid;

    table_heap->Insert(txn, inserted_tuple, &rid);
}

std::string ViewManager::GetViewDef(const std::string &view_name,
                                    Transaction *txn) {
    auto *table_heap = table_info_->table_heap_.get();
    auto schema = table_info_->schema_;
    auto table_iterator = table_heap->Begin(txn);
    std::string view_def;

    while (!table_iterator.IsEnd()) {
        Tuple tmp_tuple = table_iterator.Get();
        table_iterator++;

        std::string now_view_name = tmp_tuple.GetValue(VIEW_TABLE_NAME_FIELD, 
                                                       schema).AsString();
        
        if (view_name == now_view_name) {
            view_def = tmp_tuple.GetValue(VIEW_VIEW_DEF_FIELD,
                                          schema).AsString();
            break;
        }
    }
    

    return view_def;
}

} // namespace SimpleDB

#endif