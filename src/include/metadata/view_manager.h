#ifndef VIEW_MANAGER_H
#define VIEW_MANAGER_H

#include "metadata/table_manager.h"

namespace SimpleDB {

/**
* @brief view definitions are stored as fix-length strings
* which means that there is a relatively small limit on 
* the length of a view defition.
* In SimpleDB, the max length of a view definition is
* only 100 characters long.
* 
* View table's columns
* --------------------------------
* | view name | view definitions |
* --------------------------------
*/
class ViewManager {

    const std::string VIEW_TABLE_NAME = "view_catch";
    const std::string VIEW_TABLE_NAME_FIELD = "view_name";
    const std::string VIEW_VIEW_DEF_FIELD = "view_def";

public:

    /**
    * @brief Its constructor is called during system
    * startup and creates the view_catch table if the 
    * database is new.
    * 
    * @param IsNew Get from by filemanager
    * @param table_mgr we will share a table manager in all system
    * @param txn startup transaction
    */
    ViewManager(bool IsNew, TableManager *table_mgr, Transaction *txn);
    
    /**
    * @brief access the catalog table and 
    * insert a record into the table
    * @param view_name view  name
    * @param view_def view define
    * @param txn
    */
    void CreateView(const std::string &view_name, 
                    const std::string &view_def,
                    Transaction *txn);

    /**
    * @brief iterates through the table looking
    * for the record corresponding to the specified view name.
    * @param view_name
    * @param txn
    * @return view definition
    */
    std::string GetViewDef(const std::string &view_name,
                           Transaction *txn);

private:

    TableManager *table_mgr_;

    // cache view's tableinfo
    TableInfo *table_info_{nullptr};
};

} // namespace SimpleDB

#endif