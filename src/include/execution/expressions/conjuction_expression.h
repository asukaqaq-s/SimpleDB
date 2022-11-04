#ifndef CONJUNCTION_EXPRESSION_H
#define CONJUNCTION_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

/**
* @brief 
* ConjunctionExpression. e.g. and, or
*/
class ConjunctionExpression : public AbstractExpression {
public:
    ConjunctionExpression(ExpressionType type, AbstractExpression *left, AbstractExpression *right)
        : AbstractExpression(type, {left, right}, TypeID::INTEGER) {
        SIMPLEDB_ASSERT(left != nullptr, "null child");
        SIMPLEDB_ASSERT(right != nullptr, "null child");
        
        // check return type
        // because we only implement integer type
        // so, we use integer to replace boolean
        SIMPLEDB_ASSERT(left->GetReturnType() == TypeID::INTEGER, "type mismatch");
        SIMPLEDB_ASSERT(right->GetReturnType() == TypeID::INTEGER, "type mismatch");
    }
    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;
};

} // namespace SimpleDB

#endif