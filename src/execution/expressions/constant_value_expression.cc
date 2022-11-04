#ifndef CONSTANT_VALUE_EXPRESSION_CC
#define CONSTANT_VALUE_EXPRESSION_CC

#include "execution/expressions/constant_value_expression.h"

namespace SimpleDB {

Value ConstantValueExpression::Evaluate(const Tuple *tuple_left, 
                                        const Tuple *tuple_right) const {
    return val_;
}

} // namespace SimpleDB

#endif