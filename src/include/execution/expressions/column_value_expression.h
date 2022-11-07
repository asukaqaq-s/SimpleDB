#ifndef COLUMN_VALUE_EXPRESSION_H
#define COLUMN_VALUE_EXPRESSION_H

#include "execution/expressions/abstract_expression.h"

namespace SimpleDB {

/**
* @brief 
* ColumnValueExpression is used to generate the value based on
* value index and tuple index.
* it will be used to some term such as "F=C" and "F1=F2", 
* f is a field_name 
*/
class ColumnValueExpression : public AbstractExpression {


public:
    
    /**
    * @brief Construct a new Column Value Expression object.
    * 
    * @param ret_type Return Type
    * @param tuple_idx tuple idx, could be 0 or 1, indicating tuple is lhs or rhs
    * @param col_idx index of the column we need to extract
    * @param schema original tuple schema
    */
    ColumnValueExpression(TypeID ret_type, int tuple_idx, 
                          const std::string &field_name, 
                          Schema *schema)
        : AbstractExpression(ExpressionType::ColumnValueExpression, {}, ret_type),
          tuple_idx_(tuple_idx),
          field_name_(field_name),
          schema_(schema) {
            
        // check the value type based on schema and col idx
        SIMPLEDB_ASSERT(schema->GetColumn(field_name).GetType() == ret_type, "logic error");
    }


    /**
    * @brief columns_value_expression's evaluate method just 
    * returns the value read from tuple 
    */
    Value Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const override;
    

    inline int GetTupleIdx() const {
        return tuple_idx_;
    }

    inline std::string GetFieldName() const {
        return field_name_;
    }

private:

    // tuple idx, 0 for left tuple, 1 for right tuple
    int tuple_idx_;
    
    // field name
    std::string field_name_;
    
    // tuple schema
    Schema *schema_;
};

}

#endif