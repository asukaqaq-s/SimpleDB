#ifndef CREATE_INDEX_DATA_H
#define CREATE_INDEX_DATA_H

#include "parse/object.h"

namespace SimpleDB {

class CreateIndexData : public Object {

public:

    CreateIndexData(const std::string &index_name,
                    const std::string &table_name,
                    const std::string &field_name)
        : index_name_(index_name), table_name_(table_name),
          field_name_(field_name) {}
    
    int GetOP() override {
        return Object::CREATEINDEX;
    }

    /**
    * @brief returns the name of the index
    */
    std::string GetIndexName() const {
        return index_name_;
    }

    /**
    * @brief returns the name of the table
    */
    std::string GetTableName() const {
        return table_name_;
    }

    /**
    * @brief returns the name of the field
    */
    std::string GetFieldName() const {
        return field_name_;
    }

    std::string ToString() override {
        std::stringstream s;
        s << "index_name_ = " << index_name_ << " "
          << "table_name_ = " << table_name_ << " "
          << "field_name_ = " << field_name_ << std::endl;
        return s.str();
    }


private:

    std::string index_name_;
    
    std::string table_name_;

    std::string field_name_;

};

} // namespacee SimpleDB

#endif
