#ifndef METADATA_MANAGER_CC
#define METADATA_MANAGER_CC

#include "metadata/metadata_manager.h"

namespace SimpleDB {

std::unique_ptr<TableManager> MetadataManager::table_mgr_;
std::unique_ptr<ViewManager> MetadataManager::view_mgr_;
std::unique_ptr<StatManager> MetadataManager::stat_mgr_;
std::unique_ptr<IndexManager> MetadataManager::index_mgr_;

MetadataManager::MetadataManager(bool IsNew, Transaction *txn) {
        table_mgr_ = std::make_unique<TableManager> (IsNew, txn);
        view_mgr_ = std::make_unique<ViewManager>(IsNew, table_mgr_.get(), txn);
        stat_mgr_ = std::make_unique<StatManager>(table_mgr_.get(), txn);
        index_mgr_ = std::make_unique<IndexManager>(IsNew, table_mgr_.get(), stat_mgr_.get(), txn);
}

// set metadata 

void MetadataManager::CreateTable(std::string table_name, 
                                  Schema sch, 
                                  Transaction *txn) {
    table_mgr_->CreateTable(table_name, sch, txn);
}    

void MetadataManager::CreateView(std::string view_name, 
                                 std::string view_def, 
                                 Transaction *txn) {
    view_mgr_->CreateView(view_name, view_def, txn);
}

void MetadataManager::CreateIndex(std::string index_name, 
                                  std::string table_name, 
                                  std::string field_name, 
                                  Transaction *txn) {
    index_mgr_->CreateIndex(index_name, table_name, field_name, txn);
}

// get metadata

Layout MetadataManager::GetLayout(std::string table_name, Transaction *txn) {
    return table_mgr_->GetLayout(table_name, txn);
}


std::string MetadataManager::GetViewDef(std::string view_name, Transaction *txn) {
    return view_mgr_->GetViewDef(view_name, txn);
}


StatInfo MetadataManager::GetStatInfo(std::string table_name,
                                      Layout layout, 
                                      Transaction *txn) {
    return stat_mgr_->GetStatInfo(table_name, layout, txn);
}

std::map<std::string, IndexInfo> MetadataManager::GetIndexInfo(
    std::string table_name, 
    Transaction *txn) {
    return index_mgr_->GetIndexInfo(table_name, txn);
}


} // namespace SimpleDB

#endif