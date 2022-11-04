#ifndef OPERATOR_EXPRESSION_CC
#define OPERATOR_EXPRESSION_CC

#include "execution/expressions/operator_expression.h"

namespace SimpleDB {

Value OperatorExpression::Evaluate(const Tuple *tuple_left, 
                                   const Tuple *tuple_right) const {
    switch (type_)
    {
    case ExpressionType::OperatorExpression_NOT:
    {
        SIMPLEDB_ASSERT(children_.size() == 1, "children number error");
        Value child_value = children_[0]->Evaluate(tuple_left, tuple_right);
        if (child_value.AsInt() == true) {
            return Value(false);
        } else {
            return Value(true);
        }
        break;
    }

    case ExpressionType::OperatorExpression_IS_NULL:
    {    
        SIMPLEDB_ASSERT(children_.size() == 1, "children number error");
        Value child_value = children_[0]->Evaluate(tuple_left, tuple_right);
        if (child_value.IsNull()) {
            return Value(true);
        } else {
            return Value(false);
        }
        break;
    }

    case ExpressionType::OperatorExpression_IS_NOT_NULL:
    {
        SIMPLEDB_ASSERT(children_.size() == 1, "children number error");
        Value child_value = children_[0]->Evaluate(tuple_left, tuple_right);
        if (child_value.IsNull()) {
            return Value(false);
        } else {
            return Value(true);
        }
        break;
    }

    case ExpressionType::OperatorExpression_EXISTS:
    {
        SIMPLEDB_ASSERT(children_.size() == 1, "children number error");
        Value child_value = children_[0]->Evaluate(tuple_left, tuple_right);
        if (child_value.IsNull()) {
            return Value(false);
        } else {
            return Value(true);
        }
        break;
    }

    default:
        break;
    }


    SIMPLEDB_ASSERT(children_.size() == 2, "children number error");
    Value left_value = children_[0]->Evaluate(tuple_left, tuple_right);
    Value right_value = children_[1]->Evaluate(tuple_left, tuple_right);

    switch (type_)
    {
    case ExpressionType::OperatorExpression_Add:
        return left_value + right_value;
    
    case ExpressionType::OperatorExpression_Subtract:
        return left_value - right_value;
    
    case ExpressionType::OperatorExpression_Multiply:
        return left_value * right_value;
    
    case ExpressionType::OperatorExpression_Divide:
        return left_value / right_value;
    
    case ExpressionType::OperatorExpression_Modulo:
        return left_value % right_value;
    
    case ExpressionType::OperatorExpression_Min:
        return std::min(left_value, right_value);
    
    case ExpressionType::OperatorExpression_Max:
        return std::max(left_value, right_value);
    
    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }
    
}

} // namespace SimpleDB

#endif
