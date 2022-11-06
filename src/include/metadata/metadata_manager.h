#ifndef METADATA_MANAGER_H
#define METADATA_MANAGER_H

#include "metadata/table_manager.h"
#include "metadata/view_manager.h"
#include "metadata/stat_manager.h"
#include "metadata/index_manager.h"

#include <memory>

namespace SimpleDB {

/**
* @brief metadata manager is the client interface by
* hiding the four separate manager class.
* Instead, clients use the class MetadataMgr as the 
* single place to obtain metadata.
*/
class MetadataManager {

public:

    MetadataManager(bool IsNew, Transaction *txn, FileManager *fm, 
        RecoveryManager *rm, BufferManager *bfm);

// set metadata 

    void CreateTable(const std::string &table_name, 
                     const Schema &sch, 
                     Transaction *txn);

    void CreateView(const std::string &view_name, 
                    const std::string &view_def, 
                    Transaction *txn);

    void CreateIndex(const std::string &index_name, 
                     const std::string &table_name, 
                     const std::string &field_name, 
                     Transaction *txn);

// get metadata

    TableInfo* GetTable(const std::string &table_name, Transaction *txn);


    std::string GetViewDef(const std::string &view_name, Transaction *txn);

    StatInfo GetStatInfo(const std::string &table_name,
                         Transaction *txn);

    std::map<std::string, IndexInfo> GetIndexInfo(const std::string &table_name, 
                                                  Transaction *txn);


private:
    // why we should declare these objects as static types?

    static std::unique_ptr<TableManager> table_mgr_;

    static std::unique_ptr<ViewManager> view_mgr_;

    static std::unique_ptr<StatManager> stat_mgr_;

    static std::unique_ptr<IndexManager> index_mgr_;

};

} // namespace SimpleDB

#endif