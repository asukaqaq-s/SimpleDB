#ifndef COLUMN_VALUE_EXPRESSION_CC
#define COLUMN_VALUE_EXPRESSION_CC

#include "execution/expressions/column_value_expression.h"


namespace SimpleDB {


Value ColumnValueExpression::Evaluate(const Tuple *tuple, 
                                      const Schema &schema) const {
    SIMPLEDB_ASSERT(schema.HasColumn(column_name_), 
                    "this column not exist in the tuple");

    return tuple->GetValue(column_name_, schema);
}


Value ColumnValueExpression::EvaluateJoin
(const Tuple *left_tuple, const Schema &left_schema,
 const Tuple *right_tuple, const Schema &right_schema) const {

    bool left_has = left_schema.HasColumn(column_name_);
    bool right_has = right_schema.HasColumn(column_name_);

    if (left_has && right_has) {
        SIMPLEDB_ASSERT(false, "column exist in multipy tuples");
    }

    if (left_has) {
        return left_tuple->GetValue(column_name_, left_schema);
    }
    
    if (right_has) {
        return right_tuple->GetValue(column_name_, right_schema);
    }

    assert(false);
    return Value();
}


} // namespace SimpleDB

#endif