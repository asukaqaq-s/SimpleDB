#ifndef SELECT_STATEMENT_H
#define SELECT_STATEMENT_H

#include "execution/expressions/abstract_expression.h"
#include "parse/statement/statement.h"

#include <vector>
#include <memory>
#include <set>

namespace SimpleDB {


class SelectStatement : public Statement {

public:

    SelectStatement(const std::vector<ColumnRef> &select_list, 
                    const std::set<std::string> &tables,
                    std::shared_ptr<AbstractExpression> where,
                    // std::vector<std::unique_ptr<AbstractExpression>> group_by,
                    // std::unique_ptr<AbstractExpression> having, 
                    // std::unique_ptr<AbstractExpression> limit_count,
                    // std::unique_ptr<AbstractExpression> limit_offset,
                    // std::vector<std::unique_ptr<AbstractExpression>> sort, 
                    bool is_distinct = false
                    ) :
                    select_list_(select_list), 
                    tables_(tables), 
                    where_((where)), 
                    is_distinct_(is_distinct) {}

    StatementType GetStmtType() override {
        return StatementType::SELECT;
    }


    std::string ToString() override {
        std::string res = "select ";
        
        // for (const auto &s: select_list_) {
        //     res += s + ", ";
        // }
        
        // // remove final comma and space :", "
        // res = res.substr(0, res.size() - 2);
        // res += " from ";

        // for (const auto &s: tables_) {
        //     res += s + ", ";
        // }
        
        // remove final comma and space :", "
        // res = res.substr(0, res.size() - 2);
        // std::string pred_str = where_.ToString();
        // if (!pred_str.empty()) {
        //     res += " where " + pred_str;
        // }

        return res;
    }

    
    std::vector<ColumnRef> select_list_;

    std::set<std::string> tables_;

    std::shared_ptr<AbstractExpression> where_;

    bool is_distinct_{false};
    
};
    

} // namespace SimpleDB

#endif
