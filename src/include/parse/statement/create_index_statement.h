#ifndef CREATE_INDEX_STATEMENT_H
#define CREATE_INDEX_STATEMENT_H

#include "parse/statement/statement.h"

#include <string>

namespace SimpleDB {

class CreateIndexData : public Statement {

public:

    CreateIndexData(const std::string &index_name,
                    const std::string &table_name,
                    const std::string &field_name)
        : index_name_(index_name), 
          table_name_(table_name),
          field_name_(field_name) {}
    
    StatementType GetStmtType() override {
        return StatementType::CREATEINDEX;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "index_name_ = " << index_name_ << " "
          << "table_name_ = " << table_name_ << " "
          << "field_name_ = " << field_name_ << std::endl;
        return s.str();
    }


    std::string index_name_;
    
    std::string table_name_;

    std::string field_name_;

};

} // namespacee SimpleDB

#endif
