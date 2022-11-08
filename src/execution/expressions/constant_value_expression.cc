#ifndef CONSTANT_VALUE_EXPRESSION_CC
#define CONSTANT_VALUE_EXPRESSION_CC

#include "execution/expressions/constant_value_expression.h"

namespace SimpleDB {

Value ConstantValueExpression::Evaluate(const Tuple *tuple, 
                                        const Schema &schema) const {
    return val_;
}


Value ConstantValueExpression::EvaluateJoin
(const Tuple *left_tuple, const Schema &left_schema,
 const Tuple *right_tuple, const Schema &right_schema) const {
    
    return val_;
 }


} // namespace SimpleDB

#endif