#ifndef UPDATE_STATEMENT_H
#define UPDATE_STATEMENT_H

#include "execution/expressions/abstract_expression.h"
#include "parse/statement/statement.h"



namespace SimpleDB {

/**
* @brief data for the sql update statement
*/
class UpdateStatement :public Statement {

public:

    UpdateStatement(const std::string &table_name,
                    std::vector<std::unique_ptr<AbstractExpression>> set, 
                    std::unique_ptr<AbstractExpression> where) :
        table_name_(table_name),
        set_(std::move(set)),
        where_(std::move(where)) {}


    StatementType GetStmtType() override {
        return StatementType::UPDATE;
    }


    std::string ToString() override {
        std::stringstream s;
        // s << "table_name_ = " << table_name_ << " "
        //   << "field_name_ = " << field_name_ << " "
        //   << "new_val_ = " << new_val_.ToString() << " "
        //   << "pred = " << pred_.ToString() << std::endl;
        return s.str();
    }


    std::string table_name_;

    std::vector<std::unique_ptr<AbstractExpression>> set_;

    std::unique_ptr<AbstractExpression> where_;

};

} // namespace SimpleDB


#endif