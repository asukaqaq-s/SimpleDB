#ifndef PARSE_DELETE_H
#define PARSE_DELETE_H

#include "parse/parser.h"
#include "config/exception.h"

namespace SimpleDB {

std::unique_ptr<DeleteStatement> Parser::ParseDelete() {
    lexer_.EatKeyword("delete");
    lexer_.EatKeyword("from");

    auto table_name = lexer_.EatId();
    std::shared_ptr<AbstractExpression> where;

    if (!analyzer_->IsTableExist(table_name)) {
        throw BadSyntaxException("parse delete error: table not exist");
    }

    if (lexer_.MatchKeyword("where")) {
        where = ParseWhere();
    }

    if (!lexer_.IsEnd()) {
        throw BadSyntaxException("has other tokens not parsed");
    }

    return std::make_unique<DeleteStatement>(table_name, where);
}





} // namespace SimpleDB


#endif