#ifndef CREATE_VIEW_DATA_H
#define CREATE_VIEW_DATA_H

#include "parse/statement/statement.h"
#include "parse/statement/select_statement.h"

#include <memory>
#include <set>

namespace SimpleDB {
    
class CreateViewData : public Statement {

public:
    
    CreateViewData(const std::string &view_name,
                   std::unique_ptr<SelectStatement> select_stmt)
        : view_name_(view_name), 
          select_stmt_(std::move(select_stmt)) {}

    
    StatementType GetStmtType() override {
        return StatementType::CREATEVIEW;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "view_name_ = " << view_name_ << " "
          << "view_def = " << select_stmt_->ToString() << std::endl;
        return s.str();
    }

private:

    std::string view_name_;
    
    std::unique_ptr<SelectStatement> select_stmt_;

};

} // namespace SimpleDB

#endif
