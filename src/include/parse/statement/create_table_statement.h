#ifndef CREATE_TABLE_STATEMENT_H
#define CREATE_TABLE_STATEMENT_H

#include "record/schema.h"
#include "parse/statement/statement.h"

#include <memory>
#include <string>

namespace SimpleDB {

class CreateTableData : public Statement {

public:

    /**
    * @brief Saves the table name and schema
    */
    CreateTableData(const std::string &table_name,
                    const Schema &sch)
        : table_name_(table_name), 
          sch_(sch) {}


    StatementType GetStmtType() override {
        return StatementType::CREATETABLE;
    }


    std::string ToString() override {
        std::stringstream s;
        s << "table_name_ = " << table_name_ << std::endl;
        return s.str();
    }




    std::string table_name_;

    Schema sch_;

};

} // namespace SimpleDB

#endif