#ifndef TERM_H
#define TERM_H

#include "query/expression.h"

namespace SimpleDB {

class Plan;
/**
* @brief A term is a comparison between two expression
*/
class Term {

public:

    Term() {}
    
    Term(const Term &obj) : 
        lhs_(obj.lhs_), rhs_(obj.rhs_) {}

    /**
    * @brief Create a new term that compares two 
    * expressions for equality.
    */
    Term(Expression lhs, Expression rhs) : 
            lhs_(lhs), rhs_(rhs) { }

    /**
    * @brief return true if both of the term's expressions
    * evaluate to the same constant
    * with respect 
    */
    bool IsSatisfied(Scan *s) const;

    /**
    * @brief Calculate the extent to which selecting on 
    * the term reduces the number of records output by a 
    * query.For example if the reduction factor is 2, then 
    * the term cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */
    int ReductionFactor(Plan* plan);
    
    /**
    * @brief determine if this term is of the form "F=c" or "c=F"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param field_name the name of the field
    * @return either the constant or null
    */
    Constant EquatesWithConstant(const std::string &field_name) const;

    /**
    * @brief Determine if this term is of the form "F1=F2"
    * where is the specifed field and F2 is another field.
    * we can use this method to implement inner-join
    * @param field_name the name of the field
    * @return either the name of the other field, or null
    */
    std::string EquatesWithField(const std::string &field_name) const;

    /**
    * @brief return true if both of the term's expressions
    * apply to the specified shcema.
    * @param schema the schema
    * @return true if both expressions apply to the schema
    */
    bool AppliesTo(const Schema &schema) const;

    std::string ToString() const;

private:

    Expression lhs_;

    Expression rhs_;
};

} // namespace SimpleDB

#endif
