#ifndef CONJUCTION_EXPRESSION_CC
#define CONJUCTION_EXPRESSION_CC

#include "execution/expressions/conjuction_expression.h"

namespace SimpleDB {

Value ConjunctionExpression::Evaluate(const Tuple *tuple, 
                                      const Schema &schema) const {
                                        
    Value left_value = children_[0]->Evaluate(tuple, schema);
    Value right_value = children_[1]->Evaluate(tuple, schema);

    switch (type_)
    {
    case ExpressionType::ConjunctionExpression_AND:
        if (left_value.IsTrue() && right_value.IsTrue()) {
            return Value(true);
        } else {
            return Value(false);
        }
    
    case ExpressionType::ConjunctionExpression_OR:
        if (left_value.IsTrue() || right_value.IsTrue()) {
            return Value(true);
        } else {
            return Value(false);
        }

    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }
    assert(false);
    return Value();
}


Value ConjunctionExpression::EvaluateJoin
(const Tuple *left_tuple, const Schema &left_schema,
 const Tuple *right_tuple, const Schema &right_schema) const {
    
    Value left_value = children_[0]->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    Value right_value = children_[1]->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);

    switch (type_)
    {
    case ExpressionType::ConjunctionExpression_AND:
        if (left_value.IsTrue() && right_value.IsTrue()) {
            return Value(true);
        } else {
            return Value(false);
        }
    
    case ExpressionType::ConjunctionExpression_OR:
        if (left_value.IsTrue() || right_value.IsTrue()) {
            return Value(true);
        } else {
            return Value(false);
        }

    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }
    assert(false);
    return Value();
}





} // namespace SimpleDB

#endif