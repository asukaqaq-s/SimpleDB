#ifndef PARSER_H
#define PARSER_H

#include "record/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/operator_expression.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/delete_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/update_executor.h"
#include "parse/lexer.h"
#include "parse/stream_tokenizer.h"
#include "parse/statement/select_statement.h"
#include "parse/statement/insert_statement.h"
#include "parse/statement/delete_statement.h"
#include "parse/statement/update_statement.h"
#include "parse/statement/create_index_statement.h"
#include "parse/statement/create_table_statement.h"
#include "parse/statement/create_view_statement.h"
#include "parse/analyzer.h"


namespace SimpleDB {



class Parser {

public:

    Parser(const std::string &s, Transaction *txn, MetadataManager *mdm) 
        : lexer_(s) {
        analyzer_ = std::make_unique<Analyzer> (txn, mdm);
    }


public:

    std::unique_ptr<SelectStatement> ParseSelect();


private: // some helper functions that are often used
    
    /**
    * @brief the method for parsing column
    * @return the column which be proven a exist column
    */
    ColumnRef ParseColumn();

    /**
    * @brief the method for parsing table
    * @return the column which be proven a exist column
    */
    std::string ParseTable();    



private: // methods for parsing where clause 


    /**
    * @brief the method for parsing constant
    * @return the constant
    */
    std::shared_ptr<ConstantValueExpression> ParseConstantValue();

    /**
    * @brief the method for parsing column value
    * @return the columnvalue
    */
    std::shared_ptr<ColumnValueExpression> ParseColumnValue(int index);

    /**
    * @brief parse a columnvalue or constantvalue
    * @return a column_value_expression or constant_value_expression
    */
    std::shared_ptr<AbstractExpression> ParseLeafExpression(int index);

    /**
    * @brief Multiple arithmetic operations are not supported.
    * only support one arithmetic operations.
    * TODO:
    */
    std::shared_ptr<AbstractExpression> ParseArithmetic();


    /**
    * @brief parse a comparsion term such as F1 = F2, F1 >= 30
    * @return the comparison expression
    */
    std::shared_ptr<ComparisonExpression> ParseComparsion();


    std::shared_ptr<AbstractExpression> ParseConjuctAnd();


    std::shared_ptr<AbstractExpression> ParseConjuctOr();


    std::shared_ptr<AbstractExpression> ParseWhere();


private: // heapler functions for parsing select 

    std::vector<ColumnRef> ParseSelectList();

    std::set<std::string> ParseTableList();

    void CheckColumnLegality(ColumnRef &t);


public:
    
//     /**
//     * @brief the method for parsing query request
//     * @return querydata object which stores the query
//     */
//     std::unique_ptr<SelectStatement> ParseQuery();

//     /**
//     * @brief the method for parsing update command
//     * @return update data
//     */
//     std::unique_ptr<Object> ParseUpdateCmd();

//     /**
//     * @brief the method for parsing delete command
//     * @return the deletedata object
//     */
//     std::unique_ptr<DeleteData> ParseDelete();

//     /**
//     * @brief the method for parsing insert command
//     * @return the insertdata object
//     */
//     std::unique_ptr<InsertData> ParseInsert();

//     /**
//     * @brief the method for parsing modify command
//     * @return the modifydata object
//     */
//     std::unique_ptr<ModifyData> ParseModify(); 

//     /**
//     * @brief the method for parsing create table command
//     * @return the createtabledata object
//     */
//     std::unique_ptr<CreateTableData> ParseCreateTable();

//     /**
//     * @brief the method for parsing createview command
//     * @return the createviewdata object
//     */
//     std::unique_ptr<CreateViewData> ParseCreateView();

//     /**
//     * @brief the method for parsing createindex command
//     * @return the createindexdata object
//     */
//     std::unique_ptr<CreateIndexData> ParseCreateIndex();

// private:
    
//     // -------------------------------
//     // | methods for parsing queries |
//     // -------------------------------

//     /**
//     * @brief the method for parsing select columns list
//     * @return the array which stores the selected columns
//     */
//     std::vector<std::string> ParseSelectList();

//     /**
//     * @brief the method for parsing "from" tables list
//     * @return the array which stores the selected tables 
//     */
//     std::set<std::string> ParseTableList();

    
//     // -----------------------------------------------
//     // | methods for parsing various update commands |
//     // -----------------------------------------------

//     /**
//     * @brief each update method has a different 
//     * return type, because each one extracts
//     * different information from its command string
//     */
//     std::unique_ptr<Object> ParseCreate();

//     /**
//     * @brief the method for parsing field list
//     * @return the array which stores the field list
//     */
//     std::vector<std::string> ParseColumnList();

//     /**
//     * @brief the method for parsing inserted constant list
//     * @return the array which stores the parse 
//     */
//     std::vector<Constant> ParseConstList();


    Schema ParseColumnDefs();

    Schema ParseColumnDef();

    Schema ParseColumnType(const std::string &field_name);



private:

    Lexer lexer_;

    std::unique_ptr<Analyzer> analyzer_;

    // since a parser usually only parse one statement
    // i think cache table_list can help us check legality.
    std::set<std::string> table_list_;
};

}

#endif