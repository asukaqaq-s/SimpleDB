#ifndef PARSE_INSERT_CC
#define PARSE_INSERT_CC

#include "parse/parser.h"
#include "config/exception.h"

namespace SimpleDB {

/**
* in this file, we will parse a sql insert statement
*
* a sql insert statement format:
* 
* raw insert:
* 
* INSERT INTO table_name 
* (column1, column2, column3, column4, ...)
* VALUES
* (value1, value2, value3, value4, ...),
* (value1, value2, value3, value4, ...)...
*
* select insert:
* 
* INSERT INTO table_name
* (column1, column2, column3, column4, ...)
* SELECT column_list
* FROM table_list
* WHERE
* ...
*
*/
std::unique_ptr<InsertStatement> Parser::ParseInsert() {
    lexer_.EatKeyword("insert");
    lexer_.EatKeyword("into");

    // phase 1: parse table name
    auto table_name = ParseTable();
    
    // phase 2: parse column list
    lexer_.EatDelim('(');
    auto columns = ParseColumnList();
    lexer_.EatDelim(')');

    // phase 3: check column list legality
    if (!IsColumnListLegal(table_name, columns)) {
        throw BadSyntaxException("parse insert error: column list illegal");
    }

    // phase 4: parse values
    // maybe two cases: raw insert or select insert
    if (lexer_.MatchKeyword("select")) {
        auto subquery = ParseSelect();
        return std::make_unique<InsertStatement>(table_name, columns, std::move(subquery));
    }
    else if (lexer_.MatchKeyword("values")) {
        lexer_.EatKeyword("values");
        std::vector<Tuple> values;
        
        // insert the first tuple
        auto tmp_value_list = ParseValueList();
        auto schema = analyzer_->GetSchema(table_name);
        if (!CheckValuesLegal(table_name, tmp_value_list)) {
            throw BadSyntaxException("parse insert error: value list illegal");
        }
        values.emplace_back(Tuple(tmp_value_list, *schema));

        // continue to insert the tuple after it
        while (lexer_.MatchDelim(',')) {
            lexer_.EatDelim(',');
            auto tmp_value_list = ParseValueList();
            
            // check values legality
            if (!CheckValuesLegal(table_name, tmp_value_list)) {
                throw BadSyntaxException("parse insert error: value list illegal");
            }

            // generate a tuple and push it to values array
            values.emplace_back(Tuple(tmp_value_list, 
                                *analyzer_->GetSchema(table_name)));
        }

        // return raw insert statement
        return std::make_unique<InsertStatement> (table_name, columns, values);
    }
    
    throw BadSyntaxException("parse insert error");
    return nullptr;
}


std::vector<std::string> Parser::ParseColumnList() {
    std::vector<std::string> column_list;
    column_list.emplace_back(lexer_.EatId());

    while (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        column_list.emplace_back(lexer_.EatId());
    }
    
    return column_list;
}


std::vector<Value> Parser::ParseValueList() {
    std::vector<Value> value_list;
    lexer_.EatDelim('(');
    
    // get value list
    value_list.emplace_back(ParseConstant());
    
    while (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        value_list.emplace_back(ParseConstant());
    }
    
    lexer_.EatDelim(')');
    return value_list;
}



bool Parser::CheckValuesLegal(const std::string &table, 
                              std::vector<Value> &values) {
    auto schema = analyzer_->GetSchema(table);
    int value_num = values.size();

    if (value_num != schema->GetColumnsCount()) {
        return false;
    }

    for (int i = 0;i < value_num;i ++) {
        
        if (values[i].GetTypeID() != schema->GetColumn(i).GetType()) {

            // because default typeid of str is Varchar
            // we should update this value's typeid to char    
            if (schema->GetColumn(i).GetType() == TypeID::CHAR) {
                values[i].SetTypeID(TypeID::CHAR);
                continue;
            }
            
            return false;
        }

    }

    return true;
}


bool Parser::IsColumnListLegal(const std::string &table, 
                               const std::vector<std::string> columns) {
    auto schema = analyzer_->GetSchema(table);
    int column_num = columns.size();

    if (column_num != schema->GetColumnsCount()) {
        return false;
    }
    
    for (int i = 0;i < column_num;i ++) {
        if (columns[i] != schema->GetColumn(i).GetName()) {
            return false;
        } 
    }

    return true;
}




} // namespace SimpleDB

#endif