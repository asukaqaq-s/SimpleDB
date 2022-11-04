#ifndef COLUMN_H
#define COLUMN_H

#include "type/typeid.h"
#include "config/macro.h"
#include "file/page.h"

#include <sstream>
#include <string>

namespace SimpleDB {


/**
* @brief 
*/
class Column {

    friend class Schema;

public:

    /**
    * @brief this constructor always is used to integer or decimal type
    * because their length is constant.
    */
    Column(const std::string &column_name, TypeID column_type)
        : column_name_(column_name), column_type_(column_type) {
        
        column_length_ = GetFixedLength(column_type_);
        SIMPLEDB_ASSERT(column_type_ != TypeID::INTEGER &&
                        column_type_ != TypeID::DECIMAL, "wrong constructor");
    }

    /**
    * @brief this constructor always is used to varchar or char
    */
    Column(const std::string &column_name, TypeID column_type, int length)
        : column_name_(column_name), column_type_(column_type), column_length_(length) {
        
        SIMPLEDB_ASSERT(column_type_ != TypeID::DECIMAL &&
                        column_type_ != TypeID::INTEGER, "wrong constructor");
    }

    /**
    * @brief this constructor always ise used to tablemanage
    */
    Column(const std::string &column_name, TypeID column_type, int length, int offset)
        : column_name_(column_name), column_type_(column_type), column_length_(length), 
          column_offset_(offset) {}



    std::string GetName() const {
        return column_name_;
    }

    int GetLength() const {
        return column_length_;
    }

    int GetFixedLength() {
        return GetFixedLength(column_type_);
    }

    int GetOffset() const {
        return column_offset_;
    }

    TypeID GetType() const {
        return column_type_;
    }

    // maybe we can also inline varchar for short length?
    bool IsInlined() const {
        return column_type_ != TypeID::VARCHAR;
    }

    std::string ToString() const;
    
    // check whether two column are identical
    bool operator ==(const Column &other) const {
        return column_type_ == other.column_type_ &&
               column_length_ == other.column_length_ &&
               column_name_ == other.column_name_;
    }

    std::string ToString() {
        std::stringstream str;
        str << "column name = " << column_name_ << " "
            << "column type = " << static_cast<int>(column_type_) << " "
            << "column length = " << column_length_ << " "
            << "column offset = " << column_offset_ << std::endl;

        return str.str(); 
    }


private:

    int GetFixedLength(TypeID type_id) {
        switch (type_id) {
        case TypeID::VARCHAR:
            // store 4 byte offset pointing to real data
            return sizeof(int);
        
        case TypeID::INTEGER:
            return sizeof(int);
        
        case TypeID::DECIMAL:
            return sizeof(double);
        
        case TypeID::CHAR:
            return Page::MaxLength(column_length_);
        
        default:
            SIMPLEDB_ASSERT(false, "can't reach it");
            break;
        }
    }
    
    

private:
    
    // column name
    std::string column_name_;
    
    // column value's type
    TypeID column_type_;
    
    // the size of column, whether variable type 
    // and fix-length type, it always is same
    int column_length_{0};
    
    // column offset in the tuple
    // for fix-length type, it stores the actual data in the offset
    // otherwise, it just stores the offset of real data(4-byte-integer).
    int column_offset_{0};

};

} // namespace SimpleDB

#endif