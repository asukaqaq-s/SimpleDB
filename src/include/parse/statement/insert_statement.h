#ifndef INSERT_STATEMENT_H
#define INSERT_STATEMENT_H

#include "parse/statement/statement.h"
#include "execution/expressions/abstract_expression.h"


#include <vector>

namespace SimpleDB {

class InsertStatement : public Statement {

public:

    InsertStatement(const std::string &table_name,
                    const std::vector<std::string> &columns,
                    const std::vector<Tuple> &values) :
        table_name_(table_name), 
        columns_(columns), 
        values_(std::move(values)) {}


    InsertStatement(const std::string &table_name, 
                    const std::vector<std::string> &columns,
                    std::unique_ptr<SelectStatement> subquery) : 
        table_name_(table_name),
        columns_(columns),
        subquery_(std::move(subquery)) {}


    StatementType GetStmtType() override {
        return StatementType::INSERT;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "insert into " << table_name_ << " ( ";
        for (auto t:columns_) {
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

    std::vector<std::string> columns_;

    std::vector<Tuple> values_;

    std::unique_ptr<SelectStatement> subquery_;

};

} // namespace SimpleDB

#endif