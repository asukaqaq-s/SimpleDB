#ifndef PARSE_UPDATE_CC
#define PARSE_UPDATE_CC


#include "parse/parser.h"
#include "config/exception.h"
#include "execution/plans/update_plan.h"

namespace SimpleDB {

std::unique_ptr<UpdateStatement> Parser::ParseUpdate() {
    lexer_.EatKeyword("update");
    auto table_name = lexer_.EatId();
    table_list_.insert(table_name);
    lexer_.EatKeyword("set");

    auto set_list = ParseUpdateInfoList();
    std::shared_ptr<AbstractExpression> where;
    
    if (lexer_.MatchKeyword("where")) {
        where = ParseWhere();
    }

    if (!lexer_.IsEnd()) {
        // throw 
        throw BadSyntaxException("bad synatx!");
    }

    return std::make_unique<UpdateStatement>(table_name, set_list, where);
}

std::vector<UpdateInfoRef> Parser::ParseUpdateInfoList() {
    std::vector<UpdateInfoRef> sets;
    
    // parse the first updateinfo
    sets.emplace_back(ParseUpdateInfo());

    // parse remain updateinfo
    while (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        auto update_info = ParseUpdateInfo();
        
        // check if this column has happen, i think this is an error.
        for (auto &t:sets) {
            if (t->column_name_ == update_info->column_name_) {
                throw BadSyntaxException("parse updateinfo error: column multiple occurr");
            }
        }

        sets.emplace_back(update_info);
    }

    return sets;
}



UpdateInfoRef Parser::ParseUpdateInfo() {
    auto table_name = *table_list_.begin();
    auto column_ref = ParseColumn();
    lexer_.EatDelim('=');
    auto update_expression = ParseArithmetic();
    auto column_type = analyzer_->GetColumnType(table_name, column_ref.column_name_);

    if (column_type != update_expression->GetReturnType()) {
        if (column_type == TypeID::CHAR && 
            update_expression->GetReturnType() == TypeID::VARCHAR) {
            // ...
        }
        else {
            throw BadSyntaxException("parse update info error: type not match");
        }
    }

    return std::make_shared<UpdateInfo>(update_expression, column_ref.column_name_);
}


} // namespace SimpleDB


#endif