#ifndef METADATA_MANAGER_CC
#define METADATA_MANAGER_CC

#include "metadata/metadata_manager.h"

namespace SimpleDB {

std::unique_ptr<TableManager> MetadataManager::table_mgr_;
std::unique_ptr<ViewManager> MetadataManager::view_mgr_;
std::unique_ptr<StatManager> MetadataManager::stat_mgr_;
std::unique_ptr<IndexManager> MetadataManager::index_mgr_;

MetadataManager::MetadataManager(bool IsNew, Transaction *txn, FileManager *fm, 
        RecoveryManager *rm, BufferManager *bfm) {
        table_mgr_ = std::make_unique<TableManager> (IsNew, txn, fm, rm, bfm);
        stat_mgr_ = std::make_unique<StatManager>(table_mgr_.get(), txn);
        view_mgr_ = std::make_unique<ViewManager>(IsNew, table_mgr_.get(), txn);
        index_mgr_ = std::make_unique<IndexManager>(IsNew, table_mgr_.get(), stat_mgr_.get(), txn);
}

// set metadata 

void MetadataManager::CreateTable(const std::string &table_name, 
                                  const Schema &sch, 
                                  Transaction *txn) {
    table_mgr_->CreateTable(table_name, sch, txn);
}    

void MetadataManager::CreateView(const std::string &view_name, 
                                 const std::string &view_def, 
                                 Transaction *txn) {
    view_mgr_->CreateView(view_name, view_def, txn);
}

void MetadataManager::CreateIndex(const std::string &index_name, 
                                  const std::string &table_name, 
                                  const std::string &field_name, 
                                  Transaction *txn) {
    index_mgr_->CreateIndex(index_name, table_name, field_name, txn);
}

// get metadata

TableInfo* MetadataManager::GetTable(const std::string &table_name, Transaction *txn) {
    return table_mgr_->GetTable(table_name, txn);
}


std::string MetadataManager::GetViewDef(const std::string &view_name, Transaction *txn) {
    return view_mgr_->GetViewDef(view_name, txn);
}


StatInfo MetadataManager::GetStatInfo(const std::string &table_name,
                                      Transaction *txn) {
    return stat_mgr_->GetStatInfo(table_name, txn);
}

std::map<std::string, IndexInfo> 
MetadataManager::GetIndexInfo(const std::string &table_name, 
                              Transaction *txn) {
    return index_mgr_->GetIndexInfo(table_name, txn);
}


} // namespace SimpleDB

#endif