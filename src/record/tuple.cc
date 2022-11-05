#ifndef TUPLE_CC
#define TUPLE_CC

#include "record/tuple.h"
#include "record/column.h"

#include <cstring>
#include <sstream>

namespace SimpleDB {

Tuple::Tuple(std::vector<char> data) {
    size_ = data.size();
    auto data_ptr = std::make_shared<std::vector<char>> (data);
    data_ = std::make_unique<Page> (data_ptr);
}

Tuple::Tuple(std::vector<Value> values, const Schema &schema) {
    SIMPLEDB_ASSERT(static_cast<int>(values.size()) == 
                    schema.GetColumnsCount(), "size not match");

    int array_size = values.size();

    for (int i = 0;i < array_size; i++) {
        auto t = schema.GetColumn(i);
        SIMPLEDB_ASSERT(values[i].GetTypeID() == 
                        schema.GetColumn(i).GetType() , "type not match");
    }

    // get the size of schema
    size_ = schema.GetLength();
    
    // get the size of varlen-type's data
    // by this way, we can get the total size of tuple
    for (int i = 0;i < array_size;i ++) {
        if (values[i].GetTypeID() != TypeID::VARCHAR) {
            continue;
        }

        // we don't store data for null type
        if (values[i].IsNull()) {
            continue;
        }

        // size + data
        size_ += Page::MaxLength(values[i].GetLength());
    }


    data_ = std::make_unique<Page> (size_);

    // serialize values into the tuple
    // store the offset of varlen type
    int data_offset = schema.GetLength();
    for (int i = 0;i < array_size; i++) {
        
        // get offset
        int column_offset = schema.GetColumn(i).GetOffset();
        
        if (values[i].GetTypeID() != TypeID::VARCHAR) {
            // inline type, is not varlen type
            // just store data
            data_->SetValue(column_offset, values[i]);
        }
        else {
            // uninline type, is varlen type
            // we should store offset and data
            
            // store offset
            data_->SetInt(column_offset, data_offset);
            
            // store data
            data_->SetValue(data_offset, values[i]);

            // calc the next offset
            int max_length = values[i].GetLength();
            data_offset += Page::MaxLength(max_length);
        }
    }

    SIMPLEDB_ASSERT(data_offset == size_, "for success insert varchar");
}

Tuple::Tuple(const Tuple &other) : rid_(other.rid_),
                                   size_(other.size_) {
    if (other.data_ != nullptr) {
        data_ = std::make_unique<Page>(size_);
        memcpy(GetDataPtr(), other.GetDataPtr(), size_);
    }
    // otherwise, data will be null
}

Tuple& Tuple::operator=(Tuple other) {
    other.Swap(*this);
    return *this;
}

void Tuple::SetValue(const std::string &column_name, 
                     const Value& val,
                     const Schema &schema) {
    
    // we should not implement varchar in setvalue
    // because we should consider about changing tuple's size
    // for varchar type, always use constructor to get a new tuple
    SIMPLEDB_ASSERT(schema.GetColumn(column_name).GetType()
                    == TypeID::VARCHAR, "not implement");
    
    int offset = schema.GetColumn(column_name).GetOffset();
    data_->SetValue(offset, val);
}

Value Tuple::GetValue(const std::string &column_name,
                      const Schema &schema) const {

    int offset = schema.GetColumn(column_name).GetOffset();
    auto type = schema.GetColumn(column_name).GetType();

    switch (type)
    {
    case TypeID::INVALID:
        break;
    
    case TypeID::CHAR:
    case TypeID::INTEGER:
    case TypeID::DECIMAL:
        return Value(data_->GetValue(offset, type));
    
    case TypeID::VARCHAR: 
    {
        int data_offset = data_->GetInt(offset);
        SIMPLEDB_ASSERT(data_offset >= 0, "index should pass than 0");
        return Value(data_->GetValue(data_offset, type));
    }
    
    default:
        break;
    }

    SIMPLEDB_ASSERT(false, "should not happen");
    return Value();
}


std::string Tuple::ToString(const Schema &schema) {
    
    std::stringstream s;
    s << "{" << std::endl;
    for (auto t:schema.GetColumns()) {
        s << "      [field = " << t.GetName() << ","
          << "offset = " << t.GetOffset() << ","
          << "type = " << t.GetType() << ","
          << "length = " << t.GetLength() << ","
          << "data = " << GetValue(t.GetName(), schema).to_string() << "]" << std::endl;
    }
    s << "}";
    return s.str();
}



} // namespace SimpleDB

#endif