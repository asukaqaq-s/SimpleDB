#ifndef COMPARISON_EXPRESSION_CC
#define COMPARISON_EXPRESSION_CC

#include "execution/expressions/comparison_expression.h"


namespace SimpleDB {

Value ComparisonExpression::Evaluate(const Tuple *tuple, 
                                     const Schema &schema) const {

    SIMPLEDB_ASSERT(children_[0] != nullptr 
                    && children_[1] != nullptr, "logic error");

    Value left_value = children_[0]->Evaluate(tuple, schema);
    Value right_value = children_[1]->Evaluate(tuple, schema);

    SIMPLEDB_ASSERT(left_value.GetDataType() 
                    == right_value.GetDataType(), "datatype not match");

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


Value ComparisonExpression::EvaluateJoin
    (const Tuple *left_tuple, const Schema &left_schema,
     const Tuple *right_tuple, const Schema &right_schema) const {
    
    SIMPLEDB_ASSERT(children_[0] != nullptr 
                    && children_[1] != nullptr, "logic error");

    Value left_value = children_[0]->EvaluateJoin(left_tuple, left_schema, 
                                                  right_tuple, right_schema);
    Value right_value = children_[1]->EvaluateJoin(left_tuple, left_schema, 
                                                   right_tuple, right_schema);

    SIMPLEDB_ASSERT(left_value.GetDataType() 
                    == right_value.GetDataType(), "datatype not match");

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