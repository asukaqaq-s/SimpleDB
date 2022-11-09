#ifndef PARSE_CREATE_CC
#define PARSE_CREATE_CC

#include "parse/parser.h"
#include "config/exception.h"

namespace SimpleDB {

std::unique_ptr<CreateTableStmt> Parser::ParseCreateTable() {
    lexer_.EatKeyword("table");
    bool if_not_exist = false;
    std::vector<Column> column_list;
    
    // phase 1: parse table name
    // command: create table if not exist
    std::string table_name = lexer_.EatId();
    if (lexer_.MatchKeyword("if")) {
        lexer_.EatKeyword("if");
        lexer_.EatKeyword("not");
        lexer_.EatKeyword("exist");
        if_not_exist = true;
    }
    
    if (analyzer_->IsTableExist(table_name) && !if_not_exist) {
        throw BadSyntaxException("parse create table error: this table exists");
    }
    

    // phase 2: parse column list

    lexer_.EatDelim('(');
    column_list.emplace_back(ParseColumnDef());
    
    while (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        column_list.emplace_back(ParseColumnDef());
    }

    lexer_.EatDelim(')');


    if (!lexer_.IsEnd()) {
        throw BadSyntaxException("bad syntax!");
    }
    

    return std::make_unique<CreateTableStmt>(table_name, 
                                             std::make_shared<Schema>(column_list), 
                                             if_not_exist);
}


std::unique_ptr<CreateIndexStmt> Parser::ParseCreateIndex() {
    lexer_.EatKeyword("index");

    std::string index_name = lexer_.EatId();

    lexer_.EatKeyword("on");
    std::string table_name = lexer_.EatId();
    std::string column_name = lexer_.EatId();

    return std::make_unique<CreateIndexStmt>(index_name, table_name, column_name);
}


std::unique_ptr<CreateViewStmt> Parser::ParseCreateView() {
    lexer_.EatKeyword("view");
    std::string view_name = lexer_.EatId();

    lexer_.EatKeyword("as");
    auto qd = ParseSelect();

    return std::make_unique<CreateViewStmt>(view_name, std::move(qd));
}



Column Parser::ParseColumnDef() {
    // create a non-exist column, so we can't use ParseColumn method
    auto column_name = lexer_.EatId();
    int length = 0;
    auto type = ParseColumnType(&length);

    if (type == TypeID::VARCHAR || type == TypeID::CHAR) {
        return Column(column_name, type, length);
    }
    else {
        return Column(column_name, type);
    }
    
}


TypeID Parser::ParseColumnType(int *length) {
    if (lexer_.MatchKeyword("int")) {
        lexer_.EatKeyword("int");
        return TypeID::INTEGER;
    }
    else if (lexer_.MatchKeyword("decimal")) {
        lexer_.EatKeyword("decimal");
        return TypeID::DECIMAL;
    }
    else if (lexer_.MatchKeyword("varchar")) {
        std::cout << "varchar " << std::endl;
        lexer_.EatKeyword("varchar");
        lexer_.EatDelim('(');
        *length = lexer_.EatIntConstant();
        lexer_.EatDelim(')');
        return TypeID::VARCHAR;
    }
    else {
        lexer_.EatKeyword("char");
        lexer_.EatDelim('(');
        *length = lexer_.EatIntConstant();
        lexer_.EatDelim(')');
        return TypeID::CHAR;
    }

    assert(false);
}





} // namespace SimpleDB


#endif