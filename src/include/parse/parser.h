#ifndef PARSER_H
#define PARSER_H

#include "record/schema.h"
#include "query/predicate.h"
#include "parse/lexer.h"
#include "parse/create_index_data.h"
#include "parse/create_table_data.h"
#include "parse/create_view_data.h"
#include "parse/delete_data.h"
#include "parse/insert_data.h"
#include "parse/modify_data.h"
#include "parse/query_data.h"
#include "parse/object.h"
#include "parse/stream_tokenizer.h"


namespace SimpleDB {


class Parser {

public:

    Parser(const std::string &s) : lexer_(s) {}


    // -----------------------------------------------------------------
    // | methods for parsing predicates, terms, expressions and fields |
    // -----------------------------------------------------------------

    /**
    * @brief the method for parsing field
    * @return the field which not be proven a exist field
    */
    std::string ParseField();

    /**
    * @brief the method for parsing constant
    * @return the constant which not be proven a exist constant
    */
    Constant ParseConstant();

    /**
    * @brief the method for parsing expression
    * @return the expression object 
    */
    Expression ParseExpression();
    
    /**
    * @brief the method for parsing term
    * @return the term object
    */
    Term ParseTerm();

    /**
    * @brief the method for parsing predicate
    * @return the predicate object
    */
    Predicate ParsePred();

public:
    
    /**
    * @brief the method for parsing query request
    * @return querydata object which stores the query
    */
    std::unique_ptr<QueryData> ParseQuery();

    /**
    * @brief the method for parsing update command
    * @return update data
    */
    std::unique_ptr<Object> ParseUpdateCmd();

    /**
    * @brief the method for parsing delete command
    * @return the deletedata object
    */
    std::unique_ptr<DeleteData> ParseDelete();

    /**
    * @brief the method for parsing insert command
    * @return the insertdata object
    */
    std::unique_ptr<InsertData> ParseInsert();

    /**
    * @brief the method for parsing modify command
    * @return the modifydata object
    */
    std::unique_ptr<ModifyData> ParseModify(); 

    /**
    * @brief the method for parsing create table command
    * @return the createtabledata object
    */
    std::unique_ptr<CreateTableData> ParseCreateTable();

    /**
    * @brief the method for parsing createview command
    * @return the createviewdata object
    */
    std::unique_ptr<CreateViewData> ParseCreateView();

    /**
    * @brief the method for parsing createindex command
    * @return the createindexdata object
    */
    std::unique_ptr<CreateIndexData> ParseCreateIndex();

private:
    
    // -------------------------------
    // | methods for parsing queries |
    // -------------------------------

    /**
    * @brief the method for parsing select columns list
    * @return the array which stores the selected columns
    */
    std::vector<std::string> ParseSelectList();

    /**
    * @brief the method for parsing "from" tables list
    * @return the array which stores the selected tables 
    */
    std::set<std::string> ParseTableList();

    
    // -----------------------------------------------
    // | methods for parsing various update commands |
    // -----------------------------------------------

    /**
    * @brief each update method has a different 
    * return type, because each one extracts
    * different information from its command string
    */
    std::unique_ptr<Object> ParseCreate();

    /**
    * @brief the method for parsing field list
    * @return the array which stores the field list
    */
    std::vector<std::string> ParseFieldList();

    /**
    * @brief the method for parsing inserted constant list
    * @return the array which stores the parse 
    */
    std::vector<Constant> ParseConstList();

    Schema ParseFieldDefs();

    Schema ParseFieldDef();

    Schema ParseFieldType(const std::string &field_name);

     

private:

    Lexer lexer_;

};

}

#endif