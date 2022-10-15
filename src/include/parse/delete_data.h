#ifndef DELETE_DATA_H
#define DELETE_DATA_H

#include "query/predicate.h"
#include "parse/object.h"

namespace SimpleDB {

class DeleteData : public Object{

public:

    DeleteData(const std::string &table_name,
               const Predicate &pred) :
        table_name_(table_name), pred_(pred) {}

    int GetOP() override {
        return Object::REMOVE;
    }

    
    std::string GetTableName() const {
        return table_name_;
    }

    Predicate GetPred() const {
        return pred_;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "table_name_ = " << table_name_ << " "
          << "pred = " << pred_.ToString() <<  std::endl;
        return s.str();
    }

private:

    std::string table_name_;

    Predicate pred_;

};

} // namespace SimpleDB


#endif