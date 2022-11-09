#ifndef CREATE_INDEX_STATEMENT_H
#define CREATE_INDEX_STATEMENT_H

#include "parse/statement/statement.h"


#include <string>

namespace SimpleDB {

class CreateIndexStmt : public Statement {

public:

    CreateIndexStmt(const std::string &index_name,
                    const std::string &table_name,
                    const std::string &column_name)
        : index_name_(index_name), 
          table_name_(table_name),
          column_name_(column_name) {}
    
    StatementType GetStmtType() override {
        return StatementType::CREATEINDEX;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "index_name_ = " << index_name_ << " "
          << "table_name_ = " << table_name_ << " "
          << "column_name_ = " << column_name_ << std::endl;
        return s.str();
    }


    std::string index_name_;
    
    std::string table_name_;

    std::string column_name_;

};

} // namespacee SimpleDB

#endif
