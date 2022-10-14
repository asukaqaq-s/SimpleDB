#ifndef PARSER_CC
#define PARSER_CC

#include "parse/parser.h"

namespace SimpleDB {

std::string Parser::ParseField() {
    // a field name is a identifier
    // which is not keyword
    return lexer_.EatId();
}


Constant Parser::ParseConstant() {
    if (lexer_.MatchStringConstant()) {
        // if the token is a string
        return Constant(lexer_.EatStringConstant());
    } else {
        return Constant(lexer_.EatIntConstant());
    }
}

Expression Parser::ParseExpression() {
    // a expression is a field_name or constant
    if (lexer_.MatchId()) {
        // field_name string
        return Expression(ParseField());
    }
    else {
        // constant object
        return Expression(ParseConstant());    
    }
}

Term Parser::ParseTerm() {
    // a term is consisted of two expressions
    // term format: 1. expression 2. a symbols 3. expression  
    // so we should eat token in the steps above
    Expression lhs = ParseExpression();
    lexer_.EatDelim('=');
    Expression rhs = ParseExpression();
    return Term(lhs, rhs);
}

Predicate Parser::ParsePred() {
    // a predicate consists of one term or many terms
    // at least one term
    Predicate pred(ParseTerm());
    if (lexer_.MatchKeyword("and")) {
        lexer_.EatKeyword("and");
        // The keyword 'and' may be followed by multiple terms
        // dfs can solve this problem until not and
        pred.ConJoinWith(ParsePred());
    }
    return pred;
}

// methods for parsing queries

std::unique_ptr<QueryData> Parser::ParseQuery() {
    // a query sql statement format
    // select (fields list)
    // from (tables list)
    // [where (predicate list)]
    
    lexer_.EatKeyword("select");
    std::vector<std::string> fields = ParseSelectList();
    lexer_.EatKeyword("from");
    std::set<std::string> tables = ParseTableList();
    
    // [where] is optional 
    Predicate pred;
    if (lexer_.MatchKeyword("where")) {
        lexer_.EatKeyword("where");
        pred = ParsePred();
    }
    return std::make_unique<QueryData>(fields, tables, pred);
}

std::vector<std::string> Parser::ParseSelectList() {
    std::vector<std::string> resq;
    resq.emplace_back(ParseField());

    if (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        // dfs
        auto tmpq = ParseSelectList();
        resq.insert(resq.end(), tmpq.begin(), tmpq.end());
    }
    return resq;
}


std::set<std::string> Parser::ParseTableList() {
    std::set<std::string> ress;
    ress.insert(lexer_.EatId());

    if (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        // dfs
        auto tmps = ParseTableList();
        ress.insert(tmps.begin(), tmps.end());
    }
    return ress;
}

std::unique_ptr<Object> Parser::ParseUpdateCmd() {

    if (lexer_.MatchKeyword("insert")) {
        return ParseInsert();
    } else if (lexer_.MatchKeyword("delete")) {
        return ParseDelete();
    } else if (lexer_.MatchKeyword("update")) {
        return ParseModify();
    } else {
        return ParseCreate();
    }
}

std::unique_ptr<Object> Parser::ParseCreate() {
    // create table, view or index

    lexer_.EatKeyword("create");
    if (lexer_.MatchKeyword("table")) {
        return ParseCreateTable();
    } else if (lexer_.MatchKeyword("view")) {
        return ParseCreateView();
    } else {
        return ParseCreateIndex();
    }
}


std::unique_ptr<DeleteData> Parser::ParseDelete() {
    // a delete statement format
    // delete 
    // form (table name)
    // [where (predicate)] 
    lexer_.EatKeyword("delete");
    lexer_.EatKeyword("from");
    
    std::string table_name = lexer_.EatId();
    Predicate pred;
    if (lexer_.MatchKeyword("where")) {
        lexer_.EatKeyword("where");
        pred = ParsePred();
    }
    return std::make_unique<DeleteData> (table_name, pred);
}


std::unique_ptr<InsertData> Parser::ParseInsert() {
    // a insert statement format
    // insert into table_name (
    //    fields list
    // )
    // values (
    //    constant list    
    // )
    lexer_.EatKeyword("insert");
    lexer_.EatKeyword("into");
    std::string table_name = lexer_.EatId();
    
    /* eat field list */
    lexer_.EatDelim('(');
    std::vector<std::string> fields = ParseFieldList();
    lexer_.EatDelim(')');

    /* eat constant list */
    lexer_.EatKeyword("values");
    lexer_.EatDelim('(');
    std::vector<Constant> values = ParseConstList();
    lexer_.EatDelim(')');
    
    return std::make_unique<InsertData>(table_name, fields, values);
}

std::vector<std::string> Parser::ParseFieldList() {
    std::vector<std::string> resq;
    resq.emplace_back(ParseField());
    
    if (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        
        auto tmpq = ParseFieldList();
        resq.insert(resq.end(), tmpq.begin(), tmpq.end());
    }
    return resq;
}

std::vector<Constant> Parser::ParseConstList() {
    std::vector<Constant> resq;
    resq.emplace_back(ParseConstant());
    
    if (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        
        auto tmpq = ParseConstList();
        resq.insert(resq.end(), tmpq.begin(), tmpq.end());
    }
    return resq;
}


std::unique_ptr<ModifyData> Parser::ParseModify() {
    // a modify statement format
    // update table_name
    // set field_name = new_val
    // [where ()]
    lexer_.EatKeyword("update");
    std::string table_name = lexer_.EatId();
    lexer_.EatKeyword("set");
    std::string field_name = ParseField();
    lexer_.EatDelim('=');

    Expression new_val = ParseExpression();
    Predicate pred;
    if (lexer_.MatchKeyword("where")) {
        lexer_.EatKeyword("where");
        pred = ParsePred();
    }
    
    return std::make_unique<ModifyData> 
           (table_name, field_name, new_val, pred);
}

std::unique_ptr<CreateTableData> Parser::ParseCreateTable() {
    // format:
    // create table table_name (
    //  field_list   
    // )
    lexer_.EatKeyword("table");
    std::string table_name = lexer_.EatId();
    lexer_.EatDelim('(');
    Schema sch = ParseFieldDefs();
    lexer_.EatDelim(')');
    
    return std::make_unique<CreateTableData>(table_name, sch);
}

Schema Parser::ParseFieldDefs() {
    Schema sch = ParseFieldDef();
    if (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        Schema sch2 = ParseFieldDefs();
        sch.AddAllField(sch2);
    }
    return sch;
}

Schema Parser::ParseFieldDef() {
    std::string field_name = ParseField();
    return ParseFieldType(field_name);
}

Schema Parser::ParseFieldType(const std::string &field_name) {
    Schema sch;
    if (lexer_.MatchKeyword("int")) {
        lexer_.EatKeyword("int");
        sch.AddIntField(field_name);
    } else {
        lexer_.EatKeyword("varchar");
        lexer_.EatDelim('(');
        int strlen = lexer_.EatIntConstant();
        lexer_.EatDelim(')');
        sch.AddStringField(field_name, strlen);
    }

    return sch;
}

std::unique_ptr<CreateViewData> Parser::ParseCreateView() {
    // format:
    // create view view_name
    // as ...
    lexer_.EatKeyword("view");
    std::string view_name = lexer_.EatId();
    lexer_.EatKeyword("as");
    std::unique_ptr<QueryData> qd = ParseQuery();

    return std::make_unique<CreateViewData>(view_name, std::move(qd));
}


std::unique_ptr<CreateIndexData> Parser::ParseCreateIndex() {
    // format:
    // create index index_name
    // on table_name (
    //    field_lsit   
    // )
    lexer_.EatKeyword("index");
    std::string index_name = lexer_.EatId();
    lexer_.EatKeyword("on");
    std::string table_name = lexer_.EatId();

    lexer_.EatDelim('(');
    std::string field_name = ParseField();
    lexer_.EatDelim(')');
    return std::make_unique<CreateIndexData>(index_name, table_name, field_name);
}

} // namespace SimpleDB

#endif