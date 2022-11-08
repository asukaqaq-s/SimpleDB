#ifndef INSERT_STATEMENT_H
#define INSERT_STATEMENT_H

#include "parse/statement/statement.h"
#include "execution/expressions/abstract_expression.h"


#include <vector>

namespace SimpleDB {

class InsertStatement : public Statement {

public:

    InsertStatement(const std::string &table_name,
                    const std::vector<std::string> &fields,
                    std::vector<std::unique_ptr<AbstractExpression>> values) :
        table_name_(table_name), 
        fields_(fields), 
        values_(std::move(values)) {}

    StatementType GetStmtType() override {
        return StatementType::INSERT;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "insert into " << table_name_ << " ( ";
        for (auto t:fields_) {
            s << t;
        }
        s << ")  values ( ";
        // for (auto t:values_) {
        //     s << t.ToString() << std::endl;
        // }
        s << ")";
        return s.str();
    }



    std::string table_name_;

    std::vector<std::string> fields_;

    std::vector<std::unique_ptr<AbstractExpression>> values_;

};

} // namespace SimpleDB

#endif