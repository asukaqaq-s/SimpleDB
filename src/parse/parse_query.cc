#ifndef PARSER_CC
#define PARSER_CC

#include "parse/parser.h"
#include "config/exception.h"

#include <set>

namespace SimpleDB {


// in this file, we will parse a sql query statement

// a sql query statement format:

//      SELECT [DISTINCT] column_name,column_name
//      FROM table_name
//      [WHERE Clause]
//      [GROUP BY <group by definition>
//      [HAVING <expression> [{<operator> <expression>}…]]
//      [ORDER BY <order by definition>]
//      [LIMIT N][ OFFSET M] 
//
//



/**
* --------------------------
*       Parse Where
* --------------------------
*
* NOTETHAT: because i am lazy so not support some functions 
* such as like, between .. and, and so on.
* 
* We treat each operation as an expression, @see execution/expressions
* two basic expressions is column_name and constant_value
* we can use these expressions to implement more complex clause.
* In an expression tree, these two expressions must be leaf nodes.
* 
* a simple where clause is
* where F1 = F2 and F3 + 10 = F4 or 10 = F5
* 
* You can get the following expression tree
* 
*                 or
*                / \ 
*               /   \
*              /     \
*            and      =
*           /   \    / \ 
*          /     \  10  F5
*         /       \ 
*        =         =
*       / \       / \ 
*      /   \     /   \
*     F1   F2   +    F4
*              / \
*             F3  10
* 
*/

std::unique_ptr<SelectStatement> Parser::ParseSelect() {
    lexer_.EatKeyword("select");
    

    std::vector<ColumnRef> select_list;
    std::set<std::string> table_list; // allow repeat table
    std::shared_ptr<AbstractExpression> where;


    // Phase 1: parse select list
    select_list  = ParseSelectList();
    
    
    // Phase 2: parse table_list;
    if (!lexer_.MatchKeyword("from") ||
        select_list.empty()) {
        throw BadSyntaxException("parse select error");
    }
    lexer_.EatKeyword("from");
    table_list = ParseTableList();
    table_list_ = table_list;
    SIMPLEDB_ASSERT(table_list_.size(), "table list can't empty");
    
    
    // Phase 3: update ColumnRef and check for columnref 
    //          legality without table_name
    for (auto &t:select_list) {
        CheckColumnLegality(t);
    }
    
    // Phase 4: if a where clause is required, parsewhere
    if (lexer_.MatchKeyword("where")) {
        lexer_.EatKeyword("where");
        where = ParseWhere();
    }


    return std::make_unique<SelectStatement>
           (select_list, table_list, where);
}



std::shared_ptr<ColumnValueExpression> Parser::ParseColumnValue() {
    // check if a column value's legality
    ColumnRef columnref = ParseColumn();
    bool is_rename = columnref.table_name_.size();

    if (columnref.table_name_.empty()) {
        CheckColumnLegality(columnref);
    }
    
    auto *schema = analyzer_->GetSchema(columnref.table_name_);
    auto type = schema->GetColumn(columnref.column_name_).GetType();

    if (!is_rename) {
        return std::make_shared<ColumnValueExpression> 
               (type, columnref.column_name_);
    } else {
        // this table has been renamed, update to avoid error.
        return std::make_shared<ColumnValueExpression>
               (type, columnref.ToString());
    }
}



std::shared_ptr<ConstantValueExpression> Parser::ParseConstantValue() {
    
    return std::make_shared<ConstantValueExpression> (ParseConstant());
}


std::shared_ptr<AbstractExpression> Parser::ParseLeafExpression() {
    if (lexer_.MatchId()) {
        return ParseColumnValue();
    }
    else {
        return ParseConstantValue();
    }

    // can't reach
    throw BadSyntaxException("parse leaf expression error");
    return nullptr;
}


std::shared_ptr<AbstractExpression> Parser::ParseArithmetic() {
    // format: F1 + C or F1 + F2 or C1 + C2 or C + F1
    // C must be integer or decimal
    
    auto left = ParseLeafExpression();

    // check if need arithmetic, it's optional 
    // if true, we need to execute arithmetic op
    // otherwise, just return left node and do nothing
    if (lexer_.MatchDelim('+') || lexer_.MatchDelim('-') ||
        lexer_.MatchDelim('*') || lexer_.MatchDelim('/') ||
        lexer_.MatchDelim('%')) {
        
        char op = lexer_.GetTType();
        lexer_.EatDelim(lexer_.GetTType());


        auto right = ParseLeafExpression();
        auto left_type = left->GetReturnType();
        auto right_type = right->GetReturnType();
        
        if (left_type != right_type) {
            throw BadSyntaxException("arithmetic operation type erorr");
        }

        if (left_type == TypeID::CHAR || left_type == TypeID::VARCHAR) {
            throw BadSyntaxException("arithmetic not support string type");
        }

        switch (op)
        {
        case '+':
            return std::make_shared<OperatorExpression>
                   (ExpressionType::OperatorExpression_Add, left, right);

        case '-':
            return std::make_shared<OperatorExpression>
                   (ExpressionType::OperatorExpression_Subtract, left, right);

        case '*':
            return std::make_shared<OperatorExpression>
                   (ExpressionType::OperatorExpression_Multiply, left, right);

        case '/':
            return std::make_shared<OperatorExpression>
                   (ExpressionType::OperatorExpression_Divide, left, right);
        
        case '%':
            return std::make_shared<OperatorExpression>
                   (ExpressionType::OperatorExpression_Modulo, left, right);
        
        default:
            assert(false);
            break;
        }
    }

    return left;
}


std::shared_ptr<ComparisonExpression> Parser::ParseComparsion() {
    // we only consider about one term(F=C,F=F) in this function.
    // child tree of ComparsionExpression maybe operator_expression,
    // column_value expression or constant_value expression
    
    auto left = ParseArithmetic();
    if (lexer_.MatchDelim('=')) {
        lexer_.EatDelim('=');
        auto right = ParseArithmetic();
        return std::make_shared<ComparisonExpression>
               (ExpressionType::ComparisonExpression_Equal, left, right);
    }
    
    if (lexer_.MatchDelim('>')) {
        lexer_.EatDelim('>');
        if (lexer_.MatchDelim('=')) { /* >= */
            lexer_.EatDelim('=');
            auto right = ParseArithmetic();
            return std::make_shared<ComparisonExpression>
                   (ExpressionType::ComparisonExpression_GreaterThanEquals, left, right); 
        }
        else { /* > */
            auto right = ParseArithmetic();
            return std::make_shared<ComparisonExpression>
                   (ExpressionType::ComparisonExpression_GreaterThan, left, right);
        }
    }

    if (lexer_.MatchDelim('<')) {
        lexer_.EatDelim('<');
        if (lexer_.MatchDelim('=')) {
            lexer_.EatDelim('=');
            auto right = ParseArithmetic();
            return std::make_shared<ComparisonExpression>
                   (ExpressionType::ComparisonExpression_LessThanEquals, left, right);
        }
        else {
            auto right = ParseArithmetic();
            return std::make_shared<ComparisonExpression>
                   (ExpressionType::ComparisonExpression_LessThan, left, right);
        }
    }

    if (lexer_.MatchDelim('!')) {
        lexer_.EatDelim('!');
        if (lexer_.MatchDelim('=')) {
            lexer_.EatDelim('=');
            auto right = ParseArithmetic();
            return std::make_shared<ComparisonExpression>
                   (ExpressionType::ComparisonExpression_NotEqual, left, right);
        }
    }

    throw BadSyntaxException("parse comparison error");
    return nullptr;
}

/*
* i think it's necessary to introduce 'and', 'or'
* since the priority of AND is greater than OR.it will make some trouble in implement it.
* for example:
*    1 and 2 or 3     ==     (1 and 2) or 3
*    tree:          or
*                  / \
*                 /   \ 
*                and   3
*               /  \
*              1    2
*
*    1 or 2 and 3     ==     1 or (2 and 3)
*    tree:         or
*                 /  \
*                /    \ 
*               1      and
*                     /  \
*                    2    3
* it can be found that OR is always the parent node of AND.
*/             

std::shared_ptr<AbstractExpression> Parser::ParseConjuctAnd() {
    std::shared_ptr<AbstractExpression> res;
    std::shared_ptr<AbstractExpression> left;
    std::shared_ptr<AbstractExpression> right;

    res = ParseComparsion();
    
    while (lexer_.MatchKeyword("and")) {
        lexer_.EatKeyword("and");
        right = ParseComparsion();
        left = res;
        res = std::make_shared<ConjunctionExpression>
              (ExpressionType::ConjunctionExpression_AND, left, right);
    }

    return res;
}

std::shared_ptr<AbstractExpression> Parser::ParseConjuctOr() {
    std::shared_ptr<AbstractExpression> res;
    std::shared_ptr<AbstractExpression> left;
    std::shared_ptr<AbstractExpression> right;

    res = ParseConjuctAnd();
    
    while (lexer_.MatchKeyword("or")) {
        lexer_.EatKeyword("or");
        right = ParseConjuctAnd();
        left = res;
        res = std::make_shared<ConjunctionExpression>
              (ExpressionType::ConjunctionExpression_OR, left, right);
    }

    return res;
}

std::shared_ptr<AbstractExpression> Parser::ParseWhere() {
    return ParseConjuctOr();
}


std::vector<ColumnRef> Parser::ParseSelectList() {
    std::vector<ColumnRef> resq;
    resq.emplace_back(ParseColumn());

    while (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        resq.emplace_back(ParseColumn());
    }
    return resq;
}


std::set<std::string> Parser::ParseTableList() {
    std::set<std::string> resq;
    resq.insert(ParseTable());

    while (lexer_.MatchDelim(',')) {
        lexer_.EatDelim(',');
        resq.insert(ParseTable());
    }

    return resq;
}



void Parser::CheckColumnLegality(ColumnRef &t) {
    // not have table name
    if (t.table_name_.empty()) {
        int cnt = 0;
        std::string found_table_name; /* the table found */


        // if we not specified table name, check if there is 
        // only one table has this column
        auto it = table_list_.begin();
        for (; it != table_list_.end(); it ++) {
            
            if (analyzer_->IsColumnExist(*it, t.column_name_)) {
                cnt ++;
                found_table_name = *it;
            }

            // if there is two tables has this column, it's a exception
            if (cnt >= 2) {
                throw BadSyntaxException
                ("parse select error: multiple table has the column");
            }
        }

        t.table_name_ = found_table_name;
    }

    if (t.table_name_.empty() || t.column_name_.empty()) {
        throw BadSyntaxException("parse select error: not table has the column");
    }
}




} // namespace SimpleDB


#endif