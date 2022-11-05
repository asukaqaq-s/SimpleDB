#ifndef ABSTRACT_EXPRESSION_H
#define ABSTRACT_EXPRESSION_H

#include "record/schema.h"
#include "type/value.h"
#include "record/tuple.h"

namespace SimpleDB {

enum class ExpressionType {
    // invalid expression
    AbstractExpression,
    // used to extract value from tuple
    ColumnValueExpression,
    // used to compare the value
    ComparisonExpression_Equal,
    ComparisonExpression_NotEqual,
    ComparisonExpression_LessThan,
    ComparisonExpression_LessThanEquals,
    ComparisonExpression_GreaterThan,
    ComparisonExpression_GreaterThanEquals,
    // used to store constant value
    ConstantValueExpression,
    // used to do the arithmetical caculation
    OperatorExpression_Add,
    OperatorExpression_Subtract,
    OperatorExpression_Multiply,
    OperatorExpression_Divide,
    OperatorExpression_Modulo,
    OperatorExpression_Min,
    OperatorExpression_Max,
    OperatorExpression_NOT,
    OperatorExpression_IS_NULL,
    OperatorExpression_IS_NOT_NULL,
    OperatorExpression_EXISTS,
    // conjunction expression
    ConjunctionExpression_OR,
    ConjunctionExpression_AND,

};


/**
* @brief 
* base class of all expressions in the system.
* comments from bustub:
* Expressions are modeled as trees. i.e. every expression may 
* have a variable number of children.
*/
class AbstractExpression {
public:


    virtual ~AbstractExpression() = default;

    /**
    * @brief
    * Construct AbstractExpression object
    * @param children 
    * @param ret_type 
    */
    AbstractExpression(ExpressionType type, 
                       std::vector<const AbstractExpression *> &&children, 
                       TypeID ret_type)
        : type_(type), children_(std::move(children)), ret_type_(ret_type) {}
    

    /**
    * @brief 
    * Evaluate tuple
    * @param tuple_left 
    * @param tuple_right 
    * @return Value 
    */
    virtual Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const = 0;


    /**
    * @brief
    * Get the child expression indexed by "idx"
    * @param idx 
    * @return const AbstractExpression* 
    */
    inline const AbstractExpression *GetChildAt(uint32_t idx) const {
        SIMPLEDB_ASSERT(idx < children_.size(), "index out of bounds");
        return children_[idx];
    }


    /**
     * @brief 
     * Get all child expressions
     * @return const std::vector<const AbstractExpression *>& 
     */
    inline const std::vector<const AbstractExpression *> &GetChilren() const {
        return children_;
    }


    /**
     * @brief
     * Get the return type of current expression.
     * i.e. the type of value returned by Evaluate methods.
     * @return TypeId 
     */
    inline TypeID GetReturnType() const {
        return ret_type_;
    }


protected:
    // current expression type
    ExpressionType type_;
    
    // Children node of this expression
    std::vector<const AbstractExpression *> children_;
    
    // return type of this expression. 
    TypeID ret_type_;
};

}

#endif