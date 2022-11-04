#ifndef COMPARISON_EXPRESSION_H
#define COMPARISON_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

/**
* @brief 
* ComparisonExpression is used to express the comparison operations. e.g. X > Y, X == Y, etc.
*/
class ComparisonExpression : public AbstractExpression {

public:
    
    ComparisonExpression(ExpressionType type, const AbstractExpression *left, const AbstractExpression *right)
        : AbstractExpression(type, {left, right}, TypeID::INTEGER) {
        // check the expression type
        switch (type) {
        // fall though
        case ExpressionType::ComparisonExpression_Equal:
        case ExpressionType::ComparisonExpression_NotEqual:
        case ExpressionType::ComparisonExpression_GreaterThan:
        case ExpressionType::ComparisonExpression_GreaterThanEquals:
        case ExpressionType::ComparisonExpression_LessThan:
        case ExpressionType::ComparisonExpression_LessThanEquals:
            break;

        default:
            SIMPLEDB_ASSERT(false, "Invalid expression type for ComparisonExpression");
        }

    }
    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;
};

} // namespace SimpleDB

#endif