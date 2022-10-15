#ifndef TERM_CC
#define TERM_CC

#include "query/term.h"
#include "plan/plan.h"

namespace SimpleDB {

bool Term::IsSatisfied(Scan *s) const {

    Constant lhs_val = lhs_.Evaluate(s);
    Constant rhs_val = rhs_.Evaluate(s);
    return lhs_val == rhs_val;
}

int Term::ReductionFactor(Plan* plan) {
    // have 4 cases:
    // 1. lhs is fieldname, rhs is fieldname
    // 2. lhs is fieldname, rhs is constant
    // 3. lhs is constant, rhs is fieldname
    // 4. lhs is constant, rhs is constant
    // a constant we not need to access block
    // wheras a field we should access block

    std::string lhs_name, rhs_name;

    if (lhs_.IsFieldName() && rhs_.IsFieldName()) {
        lhs_name = lhs_.AsFieldName();
        rhs_name = lhs_.AsFieldName();
        return std::max(plan->GetDistinctVals(lhs_name),
                        plan->GetDistinctVals(rhs_name));
    }
    else if (lhs_.IsFieldName()) {
        lhs_name = lhs_.AsFieldName();
        return plan->GetDistinctVals(lhs_name);
    }
    else if (rhs_.IsFieldName()) {
        rhs_name = rhs_.AsFieldName();
        return plan->GetDistinctVals(rhs_name);
    }

    // two constant equal, we don't need to access any blocks
    if (lhs_.AsConstant() == rhs_.AsConstant())
        return 1;
    else {
        // two constant equal, the sql statement error
        return INT32_MAX;
    }
    
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
