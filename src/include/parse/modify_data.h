#ifndef MODIFY_DATA_H
#define MODIFY_DATA_H

#include "query/predicate.h"
#include "parse/object.h"

namespace SimpleDB {

/**
* @brief data for the sql update statement
*/
class ModifyData :public Object {

public:

    ModifyData(const std::string &table_name,
               const std::string &field_name,
               const Expression &new_val,
               const Predicate &pred) :
        table_name_(table_name), field_name_(field_name), 
        new_val_(new_val), pred_(pred) {}

    int GetOP() override {
        return Object::MODIFY;
    }

    std::string GetTableName() const {
        return table_name_;
    }

    std::string GetFieldNmae() const {
        return field_name_;
    }

    Expression GetNewValue() const {
        return new_val_;
    }

    Predicate GetPred() const {
        return pred_;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "table_name_ = " << table_name_ << " "
          << "field_name_ = " << field_name_ << " "
          << "new_val_ = " << new_val_.ToString() << " "
          << "pred = " << pred_.ToString() << std::endl;
        return s.str();
    }

private:

    std::string  table_name_;

    std::string field_name_;

    Expression new_val_;

    Predicate pred_;

};

} // namespace SimpleDB


#endif