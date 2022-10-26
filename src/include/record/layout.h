#ifndef LAYOUT_H
#define LAYOUT_H

#include "record/schema.h"
#include "file/page.h"

namespace SimpleDB {

/**
* @brief Description of the structure of a record.
 * It contains the name, type, length and offset of
 * each field of the table.
*/
class Layout {

public:

    Layout() = default;

    /**
    * @brief this constuctor creates a layout object from a schema.
    * this constructor is used then a table is created.It determines
    * the physical offset of each field within the record.
    * @param schema the schema of the table's records
    */    
    Layout(Schema schema) : schema_(schema) {
        int pos = 0;
        auto fields_ = schema.GetFields();
        
        // remember that we don't need to pad it
        for (std::string field_name:fields_) {
            offsets_[field_name] = pos;
            pos += LengthInBytes(field_name);
        }
        size_ = pos;
    } 

    int GetFieldCount() const {
        SIMPLEDB_ASSERT(offsets_.size() == schema_.GetFields().size(), "");
        return offsets_.size();
    }

    /**
    * Create a Layout object from the specified metadata.
    * This constructor is used when the metadata
    * is retrieved from the catalog.
    * 
    * @param schema the schema of the table's records
    * @param offsets the already-calculated offsets of the fields within a record
    * @param recordlen the already-calculated length of each record
    */
    Layout(Schema schema, std::map<std::string, int> offsets, int size) 
        : schema_(schema), offsets_(offsets), size_(size) {}

    
    Schema GetSchema() const { return schema_;}
    
    /**
    * @brief return the offset of a specified field within a record
    * 
    * @param field_name the name of the field
    * @return the offset of that field within a record
    */
    int GetOffset(std::string field_name) const { 
        
        if (offsets_.find(field_name) == offsets_.end()) {
            std::string cmd = "can not find field " + field_name;
            std::cerr << cmd << std::endl;
            SIMPLEDB_ASSERT(false, "");
        }

        return offsets_.at(field_name);
    }

    int GetLength() const { return size_; }


private:

    /**
    * @brief Get the space which how many bytes is needed
    * to store this type.
    * we just need 4-byte to store the varchar type in layout
    * and actually data will be stored in the end of tuple
    * @param field_name the name of the field
    */
    int LengthInBytes(std::string field_name) {
        TypeID type = schema_.GetType(field_name);
        
        switch(type) {
        case TypeID::INVALID:
            break;

        case TypeID::CHAR:
            return Page::MaxLength(schema_.GetFixLength(field_name));
            break;
        
        case TypeID::INTEGER:
        case TypeID::DECIMAL:
            return schema_.GetFixLength(field_name);
            break;
        
        case TypeID::VARCHAR:
            return schema_.GetFixLength(field_name);
            break;
        
        default:
            break;
        }

        return 0;
    }

    
private:

    // global schema
    Schema schema_;
    // map fieldname to the offset of field in this record/tuple
    std::map<std::string, int> offsets_;
    // the size of this tuple
    int size_;

};

} // namespace SimpleDB

#endif