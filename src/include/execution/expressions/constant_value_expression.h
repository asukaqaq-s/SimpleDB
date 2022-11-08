#ifndef CONSTANT_VALUE_EXPRESSION_H
#define CONSTANT_VALUE_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

/**
* @brief 
* Stores constant value
* it will be used to some term such as "F=C", f is a field_name 
* and c is a constant value.such as 100(integer), "213"(varchar).
*/
class ConstantValueExpression : public AbstractExpression {
public:
    ConstantValueExpression(const Value &val)
        : AbstractExpression(ExpressionType::ConstantValueExpression, {}, val.GetTypeID()),
          val_(val) {}
    
    
    Value Evaluate(const Tuple *tuple, const Schema &schema) const override;

    Value EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema,
                       const Tuple *right_tuple, const Schema &right_schema) const override;


    std::string ToString() const override {
        return val_.to_string();
    }

private:

    Value val_;
};

}

#endif