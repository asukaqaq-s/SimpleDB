#ifndef PARSER_CC
#define PARSER_CC

#include "parse/parser.h"
#include "config/exception.h"

namespace SimpleDB {


ColumnRef Parser::ParseColumn() {
    // a column name is a identifier, which is not keyword
    // support variable like table1.cola

    std::string tmp = lexer_.EatId();
    std::string table_name;
    std::string column_name;
    if (lexer_.MatchDelim('.')) {
        lexer_.EatDelim('.');
        column_name = lexer_.EatId();
        table_name = tmp;
    }
    else {
        column_name = tmp;
        table_name.clear();
    }

    if (table_name.size() &&
        !analyzer_->IsColumnExist(table_name, column_name)) {
        throw BadSyntaxException("parse column error: column not exist");
    }

    if (table_name.size()) {
        // if this column_name includes table_name
        // we need to add it to rename_list to avoid duplicate name
        rename_list_.emplace_back(ColumnRef(table_name, column_name));
    }

    return ColumnRef(table_name, column_name);
}


std::string Parser::ParseTable() {
    std::string table_name = lexer_.EatId();
    
    if (!analyzer_->IsTableExist(table_name)) {
        throw BadSyntaxException("parse table error: table not exist");
    }

    return table_name;
}


Value Parser::ParseConstant() {
    if (lexer_.MatchStringConstant()) {
        return Value(lexer_.EatStringConstant(), TypeID::VARCHAR);
    }
    
    if (lexer_.MatchIntConstant()) {
        return Value(lexer_.EatIntConstant());
    }

    if (lexer_.MatchRealConstant()) {
        return Value(lexer_.EatRealConstant());
    }

    // can't reach
    throw BadSyntaxException("parse constant value error");
    return Value();
}



std::unique_ptr<Statement> Parser::ParseModify() {
    if (lexer_.MatchKeyword("insert")) {
        return std::move(ParseInsert());
    }

    if (lexer_.MatchKeyword("delete")) {
        return std::move(ParseDelete());
    }

    if (lexer_.MatchKeyword("update")) {
        return std::move(ParseUpdate());
    }

    throw BadSyntaxException("bad syntax!");
}


std::unique_ptr<Statement> Parser::ParseCreate() {
    lexer_.EatKeyword("create");
    if (lexer_.MatchKeyword("table")) {
        return std::move(ParseCreateTable());
    }
    
    if (lexer_.MatchKeyword("index")) {
        return std::move(ParseCreateIndex());
    }

    if (lexer_.MatchKeyword("view")) {
        return std::move(ParseCreateView());
    }

    throw BadSyntaxException("bad syntax");
}


}

#endif