#ifndef COLUMN_VALUE_EXPRESSION_H
#define COLUMN_VALUE_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

/**
* @brief 
* ColumnValueExpression is used to generate the value based on column_name
* it will be used to some term such as "F=C" and "F1=F2", f is a column_name 
*/
class ColumnValueExpression : public AbstractExpression {

public:
    
    /**
    * @brief Construct a new Column Value Expression object.
    * 
    * @param ret_type Return Type
    * @param column_name find the column we need to extract
    */
    ColumnValueExpression(TypeID ret_type, 
                          const std::string &column_name)
        : AbstractExpression(ExpressionType::ColumnValueExpression, {}, ret_type),
          column_name_(column_name) {}


    /**
    * @brief columns_value_expression's evaluate method just 
    * returns the value read from tuple 
    */
    Value Evaluate(const Tuple *tuple, const Schema &schema) const override;

    
    /**
    * @brief evaluatejoin method help us find which tuple we need to evaluate
    * and get value from this tuple
    * @param left_tuple
    * @param right_tuple
    * @param left_schema the schema of left_tuple
    * @param right_schema the schema of right_tuple
    */
    Value EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema,
                       const Tuple *right_tuple, const Schema &right_schema) const override;


    inline std::string GetColumnName() const {
        return column_name_;
    }

    std::string ToString() const override {
        return column_name_;
    }


private:
    
    // column name
    std::string column_name_;
};

}

#endif