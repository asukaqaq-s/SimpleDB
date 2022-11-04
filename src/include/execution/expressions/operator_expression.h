#ifndef OPERATOR_EXPRESSION_H
#define OPERATOR_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

class OperatorExpression : public AbstractExpression {

public:
    
    OperatorExpression(ExpressionType type, AbstractExpression *left, AbstractExpression *right)
        : AbstractExpression(type, {left, right}, DeduceReturnType(type, left, right)) {}

    
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;

private:


    /**
    * @brief 
    * Deduce the return type based on operator and value type
    * @param type 
    * @param left 
    * @param right 
    * @return TypeID 
    */
    TypeID DeduceReturnType(ExpressionType type, AbstractExpression *left, AbstractExpression *right) {
        switch (type) {
        
        // fall though
        case ExpressionType::OperatorExpression_NOT:
        case ExpressionType::OperatorExpression_IS_NULL:
        case ExpressionType::OperatorExpression_IS_NOT_NULL:
        case ExpressionType::OperatorExpression_EXISTS:
            return TypeID::INTEGER;

        default: {
            
            // since we just implement integer, string, demical
            // so the type must equal or one is varchar and another one is char
            if (left->GetReturnType() == TypeID::INTEGER ||
                left->GetReturnType() == TypeID::DECIMAL) {
                
                SIMPLEDB_ASSERT(left->GetReturnType() == right->GetReturnType(),
                                "type not match");        
            }
            else {
                SIMPLEDB_ASSERT(right->GetReturnType() == TypeID::VARCHAR ||
                                right->GetReturnType() == TypeID::CHAR, "type not match");
            }

            // always turn char to varchar
            auto type = std::max(left->GetReturnType(), right->GetReturnType());

            return type;
        }
        }

    } 

    
};

} // namespace SimpleDB

#endif