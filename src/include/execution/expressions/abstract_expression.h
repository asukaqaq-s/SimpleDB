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
* @brief identify a column 
*/
class ColumnRef {
public:
    

    ColumnRef() = default;

    ColumnRef(const std::string &table_name, 
              const std::string &column_name)
        : table_name_(table_name), column_name_(column_name) {}

    std::string table_name_;

    std::string column_name_;

    std::string ToString() const {
        return table_name_+"."+column_name_;
    }
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

    using SchemaRef = std::shared_ptr<Schema>;
    using AbstractExpressionRef = std::shared_ptr<AbstractExpression>;


    virtual ~AbstractExpression() = default;

    /**
    * @brief
    * Construct AbstractExpression object
    * @param children 
    * @param ret_type 
    */
    AbstractExpression(ExpressionType type, 
                       std::vector<AbstractExpressionRef> children, 
                       TypeID ret_type)
        : type_(type), children_(std::move(children)), ret_type_(ret_type) {}
    

    /**
    * @brief 
    * Evaluate tuple which often be used to scan executor
    * @param tuple
    * @param schema 
    * @return Value 
    */
    virtual Value Evaluate(const Tuple *tuple, const Schema &schema) const = 0;

    /**
    * @brief
    * Evaluate tuple which often be used to scan executor
    * @param left_tuple
    * @param left_schema
    * @param right_tuple
    * @param right_schema
    */
    virtual Value EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema,
                               const Tuple *right_tuple, const Schema &right_schema) const = 0;


    virtual std::string ToString() const = 0;


    /**
    * @brief
    * Get the child expression indexed by "idx"
    * @param idx 
    * @return const AbstractExpression* 
    */
    inline const AbstractExpressionRef &GetChildAt(uint32_t idx) const {
        SIMPLEDB_ASSERT(idx < children_.size(), "index out of bounds");
        return children_[idx];
    }


    /**
     * @brief 
     * Get all child expressions
     * @return const std::vector<const AbstractExpression *>& 
     */
    inline const std::vector<AbstractExpressionRef> &GetChilren() const {
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


    inline ExpressionType GetExpressionType() const {
        return type_;
    }


protected:
    // current expression type
    ExpressionType type_;
    
    // Children node of this expression
    std::vector<AbstractExpressionRef> children_;
    
    // return type of this expression. 
    TypeID ret_type_;
};

}

#endif