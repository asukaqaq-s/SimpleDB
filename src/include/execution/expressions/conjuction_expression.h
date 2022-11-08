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
    ConjunctionExpression(ExpressionType type, AbstractExpressionRef left, AbstractExpressionRef right)
        : AbstractExpression(type, {left, right}, TypeID::INTEGER) {
        SIMPLEDB_ASSERT(left != nullptr, "null child");
        SIMPLEDB_ASSERT(right != nullptr, "null child");
        
        // check return type
        // because we only implement integer type
        // so, we use integer to replace boolean
        SIMPLEDB_ASSERT(left->GetReturnType() == TypeID::INTEGER, "type mismatch");
        SIMPLEDB_ASSERT(right->GetReturnType() == TypeID::INTEGER, "type mismatch");
    }
    
    Value Evaluate(const Tuple *tuple, const Schema &schema) const override;

    Value EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema,
                       const Tuple *right_tuple, const Schema &right_schema) const override;

    std::string ToString() const override {
        
        std::string left = children_[0]->ToString();
        std::string right = children_[1]->ToString();
        std::string tmp;
        switch (type_)
        {
        case ExpressionType::ConjunctionExpression_AND:
            tmp = " and ";
            break;
        case ExpressionType::ConjunctionExpression_OR:
            tmp = " or ";
            break;
        default:
            assert(false);
            break;
        }

        return left + tmp + right;
    }
};

} // namespace SimpleDB

#endif