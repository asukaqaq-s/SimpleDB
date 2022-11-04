#ifndef CONJUCTION_EXPRESSION_CC
#define CONJUCTION_EXPRESSION_CC

#include "execution/expressions/conjuction_expression.h"

namespace SimpleDB {

Value ConjunctionExpression::Evaluate(const Tuple *tuple_left, 
                                      const Tuple *tuple_right) const {
    SIMPLEDB_ASSERT(tuple_left != nullptr &&
                    tuple_right != nullptr, "evaluate error");
    
    Value left_value = children_[0]->Evaluate(tuple_left, tuple_right);
    Value right_value = children_[1]->Evaluate(tuple_left, tuple_right);

    switch (type_)
    {
    case ExpressionType::ConjunctionExpression_AND:
        if (left_value.AsInt() && right_value.AsInt()) {
            return Value(true);
        } else {
            return Value(false);
        }
    
    case ExpressionType::ConjunctionExpression_OR:
        if (left_value.AsInt() || right_value.AsInt()) {
            return Value(true);
        } else {
            return Value(false);
        }

    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }
}

} // namespace SimpleDB

#endif