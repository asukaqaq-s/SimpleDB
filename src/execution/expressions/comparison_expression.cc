#ifndef COMPARISON_EXPRESSION_CC
#define COMPARISON_EXPRESSION_CC

#include "execution/expressions/comparison_expression.h"


namespace SimpleDB {

Value ComparisonExpression::Evaluate(const Tuple *tuple_left, 
                                     const Tuple *tuple_right) const {

    SIMPLEDB_ASSERT(tuple_left != nullptr 
                    && tuple_right != nullptr, "logic error");

    Value left_value = children_[0]->Evaluate(tuple_left, tuple_right);
    Value right_value = children_[1]->Evaluate(tuple_left, tuple_right);

    SIMPLEDB_ASSERT(left_value.GetTypeID() 
                    == right_value.GetTypeID(), "typeid not match");

    switch (type_)
    {
    case ExpressionType::ComparisonExpression_Equal:
        return Value(left_value == right_value);

    case ExpressionType::ComparisonExpression_GreaterThan:
        return Value(left_value > right_value);

    case ExpressionType::ComparisonExpression_GreaterThanEquals:
        return Value(left_value >= right_value);

    case ExpressionType::ComparisonExpression_LessThan:
        return Value(left_value < right_value);

    case ExpressionType::ComparisonExpression_LessThanEquals:
        return Value(left_value <= right_value);
    
    case ExpressionType::ComparisonExpression_NotEqual:
        return Value(left_value != right_value);

    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }

    return Value();
}

} // namespace SimpleDB

#endif