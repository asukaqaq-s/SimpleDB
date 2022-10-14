#ifndef INSERT_DATA_H
#define INSERT_DATA_H

#include "query/constant.h"

#include <vector>

namespace SimpleDB {

class InsertData : public Object {

public:

    InsertData(const std::string &table_name,
               const std::vector<std::string> &fields,
               const std::vector<Constant> &values) :
        table_name_(table_name), fields_(fields), values_(values) {}

    int GetOP() override {
        return Object::INSERT;
    }

    std::string GetTableName() const {
        return table_name_;
    }

    std::vector<std::string> GetFields() const {
        return fields_;
    }

    std::vector<Constant> GetVals() const {
        return values_;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "insert into " << table_name_ << " ( ";
        for (auto t:fields_) {
            s << t;
        }
        s << ")  values ( ";
        for (auto t:values_) {
            s << t.ToString() << std::endl;
        }
        s << ")";
        return s.str();
    }

private:

    std::string table_name_;

    std::vector<std::string> fields_;

    std::vector<Constant> values_;

};

} // namespace SimpleDB

#endif