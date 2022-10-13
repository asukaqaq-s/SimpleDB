#ifndef PREDICATE_CC
#define PREDICATE_CC

#include "query/predicate.h"

namespace SimpleDB {

void Predicate::ConJoinWith(const Predicate &pred) {
    terms_.insert(terms_.end(), pred.terms_.begin(), pred.terms_.end());
}

bool Predicate::IsSatisfied(Scan *s) {
    for (const Term &t : terms_) {
        if (!t.IsSatisfied(s)) {
            return false;
        }
    }
    return true;
}

Predicate Predicate::SelectSubPred(const Schema& schema) {
    Predicate res;
    for (const auto &t : terms_) {
        if (t.AppliesTo(schema))
            res.terms_.emplace_back(t);
    }
    
    return res;
}

Predicate Predicate::JoinSubPred(
    const Schema &schema1, 
    const Schema &schema2) {
    Predicate res;
    Schema new_schema;
    
    new_schema.AddAllField(schema1);
    new_schema.AddAllField(schema2);

    // search term which can apply to the union of
    // the two specified schemas
    for (const auto &t:terms_) {
        if (!t.AppliesTo(schema1) &&
            !t.AppliesTo(schema2) &&
            t.AppliesTo(new_schema)) {
            res.terms_.emplace_back(t);
        }
    }

    return res;
}

Constant Predicate::EquatesWithConsant(const std::string &field_name) const {
    for (const auto &t : terms_) {
        auto constant = t.EquatesWithConstant(field_name);
        if (!constant.IsNull())
            return constant;
    }
    return Constant();
}

std::string Predicate::EquatesWithField(const std::string &field_name) const {
    for (const auto &t : terms_ ) {
        std::string s = t.EquatesWithField(field_name);
        if (!s.empty())
            return s;
    }
    return "";
}

std::string Predicate::ToString() const {
    std::string res;
    int cnt = 0;
    
    for (const Term &t : terms_) {
        cnt ++;
        res += t.ToString();
        
        if (cnt < static_cast<int>(terms_.size())) {
            res += " and ";
        }
    }

    return res;
}

} // namespace SimpleDB

#endif