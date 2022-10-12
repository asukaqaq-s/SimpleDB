#ifndef PREDICATE_H
#define PREDICATE_H

#include "query/term.h"

#include <list>

namespace SimpleDB {

/**
* @brief a predicate is a bool combination of terms
*/
class Predicate {

public:

    Predicate() {}

    Predicate(const Term &t) { terms_.emplace_back(t); }

    /**
    * @brief modifies the predicate to be the conjunction
    * of itself and the specified predicate
    * @param pred the other predicate
    */
    void ConJoinWith(const Predicate &pred);

    /**
    * @brief return true if the predicate evaluates to
    * true with respect to the specified scan.
    * @param s the scan
    * @return true if the predicate is true in the scan
    */
    bool IsSatisfied(Scan *s);

    // reductionFactor(Plan p)

    /**
    * @brief return the subpredicate that applies to
    * the specified schema.
    * @param schema the schema
    * @return the subpredicate applying to the schema
    */
    Predicate SelectSubPred(const Schema &schema);

    /**
    * @brief return the sub predicate consisting of terms that
    * apply to the union of the two specified schemas,
    * but not to either schema separately.
    * @param schema1 the first schema
    * @param schema2 the second schema
    * @return the subpredicate whose terms apply to the union
    */
    Predicate JoinSubPred(const Schema &schema1, const Schema &schema2);

    /**
    * @brief determine if there is a term of the form "F=C"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null
    * @param field_name the name of the field
    * @return either the constant or null
    */
    Constant EquatesWithConsant(const std::string &field_name) const;


    /**
    * @brief determine if there is a term of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * @param field_anme the name of the field
    * @return the name of the other field, or null
    */
    std::string EquatesWithField(const std::string &field_name) const;

    /**
    * debugging
    */
    std::string ToString();
private:

    std::list<Term> terms_;

};

} // namespace SimpleDB

#endif