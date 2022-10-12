#ifndef SCHEMA_H
#define SCHEMA_H

#include "config/field_type.h"
#include "config/macro.h"

#include <vector>
#include <string>
#include <algorithm>
#include <map>
#include <iostream>

namespace SimpleDB {

/**
* @brief the name and type of each field
* A schema can be thought of as a list of 
* triples of the form [fieldname, type, length]. 
*/
class Schema {


/**
* @brief Field represents a column in a record
* and FieldInfo contains some infomation about this column
*/
class FieldInfo {

public:
    FieldInfo() = default;
    FieldInfo(FieldType type, int length) : 
        type_(type), length_(length) {} 

    FieldInfo & operator =(const FieldInfo &p) {
        type_ = p.type_;
        length_ = p.length_;
        return  *this;
    }

    FieldType type_;
    int length_;    
};

public:
    
    Schema() = default;
    
    ~Schema() = default;

    /**
    * @brief Add a field to the schema having a specified 
    * name, type, and length
    * If the field type is "INTEGER", then the length value is irrelevant
    * @param field_name the name of field
    * @param type the type of the field, according to the constants in simpledb
    * @param length the conceptual length of a string field
    */
    void AddField(std::string field_name, FieldType type, int length) {
        fields_.emplace_back(field_name);
        info_[field_name] = FieldInfo(type, length);     
    }

    /**
    * @brief because the size of int field is unnecessary
    * the method AddIntField gives intergers a length value of 0,
    * @param field_name the name of the field
    */
    void AddIntField(std::string field_name) {
        AddField(field_name, FieldType::INTEGER, 0);
    }

    /**
    * @brief 
    * @param field_name the name of the field
    * @param length the number of chars in the varchar definition
    */
    void AddStringField(std::string field_name, int length) {
        AddField(field_name, FieldType::VARCHAR, length);
    }

    /**
    * @brief Add a field to the shcema having the same
    * type and length as the corresponding field
    * in another schema
    * @param field_name the name of the field
    * @param schema the other schema
    */
    void AddSameField(std::string field_name, const Schema &schema) {
        FieldType type = schema.GetType(field_name);
        int length = schema.GetLength(field_name);
        AddField(field_name, type, length);
    }

    /**
    * Add all of the fields in the specified schema
    * to the current schema.
    * @param sch the other schema
    */
    void AddAllField(const Schema &schema) {
        auto fields = schema.GetFields();
        for (auto t:fields) {
            AddSameField(t, schema);
        }
    }


    std::vector<std::string> GetFields() const { return fields_;}

    bool HasField(const std::string &field_name) const { 
        return std::find(fields_.begin(), fields_.end(), field_name) 
                !=  fields_.end();
    }
    
    FieldType GetType(const std::string &field_name) const {
        SIMPLEDB_ASSERT(info_.find(field_name) != info_.end(), "");
        return info_.at(field_name).type_;
    }
    
    int GetLength(const std::string &field_name) const {
        SIMPLEDB_ASSERT(info_.find(field_name) != info_.end(), "");
        return info_.at(field_name).length_;
    }

private:

    // the list of fields name
    std::vector<std::string> fields_;
    // map fieldname to field
    std::map<std::string, FieldInfo> info_;
};

} // namespace SimpleDB

#endif