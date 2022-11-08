#ifndef OPERATOR_EXPRESSION_CC
#define OPERATOR_EXPRESSION_CC

#include "execution/expressions/operator_expression.h"

namespace SimpleDB {

Value OperatorExpression::Evaluate(const Tuple *tuple, 
                                   const Schema &schema) const {
    switch (type_)
    {
    case ExpressionType::OperatorExpression_NOT:
    {
        SIMPLEDB_ASSERT(children_.size() == 1, "children number error");
        Value child_value = children_[0]->Evaluate(tuple, schema);
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
        Value child_value = children_[0]->Evaluate(tuple, schema);
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
        Value child_value = children_[0]->Evaluate(tuple, schema);
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
        Value child_value = children_[0]->Evaluate(tuple, schema);
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
    Value left_value = children_[0]->Evaluate(tuple, schema);
    Value right_value = children_[1]->Evaluate(tuple, schema);

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
    
    return Value();
}



Value OperatorExpression::EvaluateJoin
(const Tuple *left_tuple, const Schema &left_schema,
 const Tuple *right_tuple, const Schema &right_schema) const {
    
    switch (type_)
    {
    case ExpressionType::OperatorExpression_NOT:
    {
        SIMPLEDB_ASSERT(children_.size() == 1, "children number error");
        Value child_value = children_[0]->EvaluateJoin(left_tuple, left_schema, 
                                                       right_tuple, right_schema);
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
        Value child_value = children_[0]->EvaluateJoin(left_tuple, left_schema, 
                                                       right_tuple, right_schema);
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
        Value child_value = children_[0]->EvaluateJoin(left_tuple, left_schema, 
                                                       right_tuple, right_schema);
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
        Value child_value = children_[0]->EvaluateJoin(left_tuple, left_schema, 
                                                       right_tuple, right_schema);
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
    Value left_value = children_[0]->EvaluateJoin(left_tuple, left_schema, 
                                                  right_tuple, right_schema);
    Value right_value = children_[1]->EvaluateJoin(left_tuple, left_schema, 
                                                  right_tuple, right_schema);

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
    
    return Value();
}




} // namespace SimpleDB

#endif
