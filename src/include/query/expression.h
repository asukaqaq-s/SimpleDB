#ifndef EXPRESSION_H
#define EXPRESSION_H

#include "record/schema.h"
#include "query/constant.h"
#include "query/scan.h"

namespace SimpleDB {

/**
* @brief the class corresponding to SQL exception
*/
class Expression {

public:

    Expression() {}

    Expression(const Constant &val) : val_(val) {}

    Expression(const std::string &field_name) : 
               field_name_(field_name) { }

    /**
    * @brief Evaluate the expression with respect to the
    * current record of the specified scan.
    * @param s the scan
    * @return the value of the expression, as a constant
    */
    Constant Evaluate(Scan* s) const {
        return (val_.IsNull()) ? s->GetVal(field_name_) : val_;
    }

    /**
    * @brief return true if the expression is a field reference
    */
    bool IsFieldName() const { return !field_name_.empty(); }
    
    /**
    * @brief return the constant corresponding to a
    * constant expression, or null is the expression does
    * not denote a constant
    * @return the expression as a constant
    */
    Constant AsConstant() const { return val_;}

    /**
    * @brief return the field anme corresponding to a constant
    * expression, or null if the expression dores not denote a field
    * @return the expression as a field name
    */
    std::string AsFieldName() const { return field_name_; }

    /**
    * @brief Determine if all of the fields mentioned in this 
    * expression are contained in the specified schema.
    * used by the query planner to determine the scope of the 
    * expression.
    * @param schema the schema
    * @return true if all fields in the expression are in the schema
    */
    bool AppliesTo(const Schema &schema) const {
        return (val_.IsNull()) ? schema.HasField(field_name_) : true;
    }

    std::string ToString() const { 
        return (val_.IsNull()) ? field_name_ : val_.ToString();
    }

private:

    Constant val_;

    std::string field_name_;
};

} // namespace SimpleDB

#endif
