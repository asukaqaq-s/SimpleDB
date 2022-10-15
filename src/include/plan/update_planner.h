#ifndef UPDATE_PLANNER_H
#define UPDATE_PLANNER_H

#include "concurrency/transaction.h"
#include "plan/plan.h"
#include "parse/insert_data.h"
#include "parse/delete_data.h"
#include "parse/modify_data.h"
#include "parse/create_index_data.h"
#include "parse/create_table_data.h"
#include "parse/create_view_data.h"


namespace SimpleDB {

class UpdatePlanner {

public:

    virtual ~UpdatePlanner() = default;

    /**
    * @brief Executes the specified insert statement, and
    * returns the number of affected records.
    * @param data
    * @param txn
    */
    virtual int ExecuteInsert(InsertData *data, Transaction *txn) = 0;

    /**
    * @brief Executes the specified delete statement, and
    * returns the number of affected records.
    * @param data
    * @param txn
    */
    virtual int ExecuteDelete(DeleteData *data, Transaction *txn) = 0;

    /**
    * @brief Executes the specified modify statement, and
    * returns the number of affected records.
    * @param data
    * @param txn
    */
    virtual int ExecuteModify(ModifyData *data, Transaction *txn) = 0;

    /**
    * @brief Executes the specified create table statement, and
    * returns the number of affected records.
    * @param data
    * @param txn
    */
    virtual int ExecuteCreateTable(CreateTableData *data, Transaction *txn) = 0;

    /**
    * @brief Executes the specified create view statement, and
    * returns the number of affected records.
    * @param data
    * @param txn
    */
    virtual int ExecuteCreateView(CreateViewData *data, Transaction *txn) = 0;

    /**
    * @brief Executes the specified create index statement, and
    * returns the number of affected records.
    * @param data
    * @param txn
    */
    virtual int ExecuteCreateIndex(CreateIndexData *data, Transaction *txn) = 0;


};

}

#endif