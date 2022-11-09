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

    /**
    * @brief 
    */
    std::unique_ptr<SelectStatement> ParseSelect();

    /**
    * @brief
    */
    std::unique_ptr<Statement> ParseModify();

    /**
    * @brief
    */
    std::unique_ptr<Statement> ParseCreate();

    /**
    * @brief not implement 
    */
    // std::unique_ptr<Statement> ParseDrop();


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

    /**
    * 
    */
    Value ParseConstant();



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
    std::shared_ptr<ColumnValueExpression> ParseColumnValue();

    /**
    * @brief parse a columnvalue or constantvalue
    * @return a column_value_expression or constant_value_expression
    */
    std::shared_ptr<AbstractExpression> ParseLeafExpression();

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


private: // healper functions for parsing modify

    std::unique_ptr<InsertStatement> ParseInsert();

    std::unique_ptr<DeleteStatement> ParseDelete() { return nullptr;}

    std::unique_ptr<UpdateStatement> ParseUpdate() { return nullptr;}

    std::vector<std::string> ParseColumnList();

    std::vector<Value> ParseValueList();

    bool CheckValuesLegal(const std::string &table, 
                          std::vector<Value> &columns);

    bool IsColumnListLegal(const std::string &table, 
                           const std::vector<std::string> columns);

private:

    Lexer lexer_;

    std::unique_ptr<Analyzer> analyzer_;

    // since a parser usually only parse one statement
    // i think cache table_list can help us check legality.
    std::set<std::string> table_list_;

    // since a parser usually only parse one statement
    // i think cache rename_list can help us make select
    std::vector<ColumnRef> rename_list_;
};

}

#endif