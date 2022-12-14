#ifndef DELETE_STATEMENT_H
#define DELETE_STATEMENT_H

#include "execution/expressions/abstract_expression.h"
#include "parse/statement/statement.h"

namespace SimpleDB {

class DeleteStatement : public Statement {

public:

    DeleteStatement(const std::string &table_name,
                    std::shared_ptr<AbstractExpression> where) :
                    table_name_(table_name), 
                    where_(where) {}


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

    std::shared_ptr<AbstractExpression> where_;

};

} // namespace SimpleDB


#endif