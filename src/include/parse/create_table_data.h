#ifndef CREATE_TABLE_DATA_H
#define CREATE_TABLE_DATA_H

#include <memory>
#include <string>

#include "parse/object.h"
#include "record/schema.h"

namespace SimpleDB {

class CreateTableData : public Object {

public:

    int GetOP() override {
        return Object::CREATETABLE;
    }

    /**
    * @brief Saves the table name and schema
    */
    CreateTableData(const std::string &table_name,
                    const Schema &sch)
        : table_name_(table_name), sch_(sch) {}

    /**
    * @brief return the name of the new table
    */
    std::string GetTableName() const {
        return table_name_;
    }

    /**
    * @brief return the schema of the new table
    */
    Schema GetSchema() const {
        return sch_;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "table_name_ = " << table_name_ << std::endl;
        return s.str();
    }

private:

    std::string table_name_;

    Schema sch_;

};

} // namespace SimpleDB

#endif