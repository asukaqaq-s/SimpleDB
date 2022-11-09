#ifndef CREATE_TABLE_STATEMENT_H
#define CREATE_TABLE_STATEMENT_H

#include "record/schema.h"
#include "parse/statement/statement.h"

#include <memory>
#include <string>

namespace SimpleDB {

class CreateTableStmt : public Statement {

public:

    /**
    * @brief Saves the table name and schema
    */
    CreateTableStmt(const std::string &table_name,
                    const SchemaRef &sch,
                    bool table_exist = false)
        : table_name_(table_name), 
          sch_(sch),
          is_table_exist_(table_exist) {}


    StatementType GetStmtType() override {
        return StatementType::CREATETABLE;
    }


    std::string ToString() override {
        std::stringstream s;
        s << "table_name_ = " << table_name_ << std::endl;
        return s.str();
    }




    std::string table_name_;

    SchemaRef sch_;

    bool is_table_exist_;

};

} // namespace SimpleDB

#endif