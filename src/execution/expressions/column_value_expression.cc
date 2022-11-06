#ifndef COLUMN_VALUE_EXPRESSION_CC
#define COLUMN_VALUE_EXPRESSION_CC

#include "execution/expressions/column_value_expression.h"


namespace SimpleDB {


Value ColumnValueExpression::Evaluate(const Tuple *tuple_left, 
                                      const Tuple *tuple_right) const {
    
    SIMPLEDB_ASSERT(tuple_idx_ == 0 || tuple_idx_ == 1, "tuple index error");
    
    if (tuple_idx_ == 0) {
        return tuple_left->GetValue(field_name_, *schema_);
    } else {
        return tuple_right->GetValue(field_name_, *schema_);
    }
}

} // namespace SimpleDB

#endif