#ifndef OPERATOR_EXPRESSION_H
#define OPERATOR_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

class OperatorExpression : public AbstractExpression {

public:
    
    OperatorExpression(ExpressionType type, AbstractExpressionRef left, AbstractExpressionRef right)
        : AbstractExpression(type, {left, right}, DeduceReturnType(type, left, right)) {}

    
    Value Evaluate(const Tuple *tuple, const Schema &schema) const override;


    Value EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema,
                       const Tuple *right_tuple, const Schema &right_schema) const override;



    std::string ToString() const override {
        std::string left = children_[0]->ToString();
        std::string right = children_[1]->ToString();
        std::string tmp;


        switch (type_)
        {
        case ExpressionType::OperatorExpression_Add:
            tmp = " + ";
            break;
        
        case ExpressionType::OperatorExpression_Subtract:
            tmp = " - ";
            break;
        
        case ExpressionType::OperatorExpression_Multiply:
            tmp = " * ";
            break;
        
        case ExpressionType::OperatorExpression_Divide:
            tmp = " / ";
            break;
        
        case ExpressionType::OperatorExpression_Modulo:
            tmp = " % ";
            break;
        
        case ExpressionType::OperatorExpression_Min:
            SIMPLEDB_ASSERT(false, "not implement");
            break;
        
        case ExpressionType::OperatorExpression_Max:
            SIMPLEDB_ASSERT(false, "not implement");
            break;
        
        default:
            SIMPLEDB_ASSERT(false, "can't reach");
            break;
        }


        
        return left + tmp + right;
    }


private:


    /**
    * @brief 
    * Deduce the return type based on operator and value type
    * @param type 
    * @param left 
    * @param right 
    * @return TypeID 
    */
    TypeID DeduceReturnType(ExpressionType type, AbstractExpressionRef left, AbstractExpressionRef right) {
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