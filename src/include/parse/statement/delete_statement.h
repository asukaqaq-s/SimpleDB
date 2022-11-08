#ifndef DELETE_STATEMENT_H
#define DELETE_STATEMENT_H

#include "execution/expressions/abstract_expression.h"
#include "parse/statement/statement.h"

namespace SimpleDB {

class DeleteData : public Statement {

public:

    DeleteData(const std::string &table_name,
               std::unique_ptr<AbstractExpression> where) :
        table_name_(table_name), 
        where_(std::move(where)) {}


    StatementType GetStmtType() override {
        return StatementType::DELETE;
    }


    std::string ToString() override {
        std::stringstream s;
        // s << "table_name_ = " << table_name_ << " " 
        //   << "pred = " << pred_.ToString() <<  std::endl;
        return s.str();
    }

    std::string table_name_;

    std::unique_ptr<AbstractExpression> where_;

};

} // namespace SimpleDB


#endif