#ifndef SCHEMA_H
#define SCHEMA_H

#include "type/typeid.h"
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
* we will need a schema object to calulate offset in layout 
*/
class Schema {


/**
* @brief Field represents a column in a record
* and FieldInfo contains some infomation about this column
*/
class FieldInfo {

public:
    FieldInfo() = default;
    FieldInfo(TypeID type, int length) : 
        type_(type), length_(length) {} 

    FieldInfo& operator =(const FieldInfo &p) {
        type_ = p.type_;
        length_ = p.length_;
        return  *this;
    }

    TypeID type_;

    // for string type, length_ means the maxsize
    // of the field
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
    void AddField(const std::string &field_name, TypeID type, int length) {
        fields_.emplace_back(field_name);
        info_[field_name] = FieldInfo(type, length);     
    }

    /**
    * @brief because the size of int field is unnecessary
    * the method AddIntField gives intergers a length value of 0,
    * @param field_name the name of the field
    */
    void AddIntField(const std::string &field_name) {
        AddField(field_name, TypeID::INTEGER, 0);
    }

    /**
    * @brief because the size of double field is unnecessary
    * the method AddDemField gives decimals a length value of 0,
    * @param field_name the name of the field
    */
    void AddDemField(const std::string &field_name) {
        AddField(field_name, TypeID::DECIMAL, 0);
    }

    /**
    * @brief 
    * @param field_name the name of the field
    * @param length the number of chars in the varchar definition
    */
    void AddStrField(std::string field_name, int length) {
        AddField(field_name, TypeID::CHAR, length);
    }

    void AddVarStrField(std::string field_name, int length) {
        AddField(field_name, TypeID::VARCHAR, length);
    }

    /**
    * @brief Add a field to the shcema having the same
    * type and length as the corresponding field
    * in another schema
    * @param field_name the name of the field
    * @param schema the other schema
    */
    void AddSameField(std::string field_name, const Schema &schema) {
        TypeID type = schema.GetType(field_name);
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

    std::vector<std::string> GetFields() const { 
        return fields_;
    }

    bool HasField(const std::string &field_name) const { 
        return std::find(fields_.begin(), fields_.end(), field_name) 
                !=  fields_.end();
    }
    
    TypeID GetType(const std::string &field_name) const {
        SIMPLEDB_ASSERT(info_.find(field_name) != info_.end(), "");
        return info_.at(field_name).type_;
    }

    /**
    * @brief the method will get the maxsize of string type 
    */
    int GetLength(const std::string &field_name) const {
        SIMPLEDB_ASSERT(info_.find(field_name) != info_.end(), "");
        return info_.at(field_name).length_;
    }

    /**
    * @brief the method will be called by layout object for calc
    * the fix-len size
    */
    int GetFixLength(const std::string &field_name) const {
        SIMPLEDB_ASSERT(info_.find(field_name) != info_.end(), "");
        auto type = info_.at(field_name).type_;

        if (type == TypeID::VARCHAR) {
            return sizeof(int);
        } else {
            switch (type)
            {
            case TypeID::CHAR:
                return info_.at(field_name).length_;
                break;
            
            case TypeID::INTEGER:
                return sizeof(int);
                break;
            
            case TypeID::DECIMAL:
                return sizeof(double);
                break;
                
            default:
                SIMPLEDB_ASSERT(false, "");
            }
        }
    }

private:

    // the list of fields name
    std::vector<std::string> fields_;
    // map fieldname to field
    std::map<std::string, FieldInfo> info_;
};

} // namespace SimpleDB

#endif