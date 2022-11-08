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
    
    ComparisonExpression(ExpressionType type, AbstractExpressionRef left, AbstractExpressionRef right)
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
    
    Value Evaluate(const Tuple *tuple, const Schema &schema) const override;

    Value EvaluateJoin(const Tuple *left_tuple, const Schema &letf_schema,
                       const Tuple *right_tuple, const Schema &right_schema) const override;
    

    std::string ToString() const override {
        std::stringstream s;
        s << children_[0]->ToString();
        s << " ";

        switch (type_)
        {
        case ExpressionType::ComparisonExpression_Equal:
            s << "=";
            break;
        case ExpressionType::ComparisonExpression_NotEqual:
            s << "!=";
            break;
        case ExpressionType::ComparisonExpression_GreaterThan:
            s << ">";
            break;
        case ExpressionType::ComparisonExpression_GreaterThanEquals:
            s << ">=";
            break;
        case ExpressionType::ComparisonExpression_LessThan:
            s << "<";
            break;
        case ExpressionType::ComparisonExpression_LessThanEquals:
            s << "<=";
            break;
        default:
            break;
        }

        s << " " << children_[1]->ToString();
        return s.str();
    }
};

} // namespace SimpleDB

#endif