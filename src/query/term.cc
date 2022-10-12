#ifndef TERM_CC
#define TERM_CC

#include "query/term.h"

namespace SimpleDB {

bool Term::IsSatisfied(Scan *s) const {

    Constant lhs_val = lhs_.Evaluate(s);
    Constant rhs_val = rhs_.Evaluate(s);
    return lhs_val == rhs_val;
}

Constant Term::EquatesWithConstant(
    const std::string &field_name) const {
    if (lhs_.IsFieldName() && 
        lhs_.AsFieldName() == field_name &&
        !rhs_.IsFieldName()) {

        return rhs_.AsConstant();
    }
    else if (rhs_.IsFieldName() &&
             rhs_.AsFieldName() == field_name &&
             !lhs_.IsFieldName()) {
        
        return lhs_.AsConstant();
    }
    else {
        return Constant();
    }
}

std::string Term::EquatesWithField(
    const std::string &field_name) const {
    if (lhs_.IsFieldName() &&
        lhs_.AsFieldName() == field_name &&
        rhs_.IsFieldName()) {
        
        return rhs_.AsFieldName();        
    } else if (rhs_.IsFieldName() &&
               rhs_.AsFieldName() == field_name &&
               lhs_.IsFieldName()) {

        return lhs_.AsFieldName();
    }
    return "";
}


bool Term::AppliesTo(const Schema &schema) const {
    return lhs_.AppliesTo(schema) && 
           rhs_.AppliesTo(schema);
}

std::string Term::ToString() const {
    return lhs_.ToString() + "=" + rhs_.ToString();
}

} // namespace SimpleDB


#endif
