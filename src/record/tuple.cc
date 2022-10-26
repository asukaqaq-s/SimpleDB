#ifndef TUPLE_CC
#define TUPLE_CC

#include "record/tuple.h"

#include <cstring>
#include <sstream>

namespace SimpleDB {

Tuple::Tuple(std::vector<char> data) {
    size_ = data.size();
    auto data_ptr = std::make_shared<std::vector<char>> (data);
    data_ = std::make_unique<Page> (data_ptr);
}

Tuple::Tuple(std::vector<Constant> values, const Layout &layout) {
    SIMPLEDB_ASSERT(static_cast<int>(values.size()) == 
                    layout.GetFieldCount(), "size not match");

    Schema schema = layout.GetSchema();
    auto fields = schema.GetFields();
    int array_size = values.size();

    for (int i = 0;i < array_size; i++) {
        SIMPLEDB_ASSERT(values[i].GetTypeID() == 
                        schema.GetType(fields[i]), "type not match");
    }

    // calculate the size of tuple
    // for string type, if it's null, the size would be 4
    // otherwise it's 4 + length
    size_ = layout.GetLength();
    
    // calc the total length of varlen type
    for (int i = 0;i < static_cast<int>(values.size());i ++) {
        if (values[i].GetTypeID() != TypeID::VARCHAR) {
            continue;
        }
        // we don't store data for null type
        if (values[i].IsNull()) {
            continue;
        }
        // size + data
        size_ += Page::MaxLength(schema.GetLength(fields[i]));
    } 

    data_ = std::make_unique<Page> (size_);

    // serialize values into the tuple
    // store the offset of varlen type
    int data_offset = layout.GetLength();
    for (int i = 0;i < array_size; i++) {
        int field_offset = layout.GetOffset(fields[i]);
        if (values[i].GetTypeID() != TypeID::VARCHAR) {
            // inline type, is not varlen type
            // just store data
            data_->SetValue(field_offset, values[i]);
        }
        else {
            // uninline type, is varlen type
            // we should store offset and data
            
            // store offset
            data_->SetInt(field_offset, data_offset);
            
            // store data
            data_->SetValue(field_offset, values[i]);

            // calc the next offset
            int max_length = schema.GetLength(fields[i]);
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

void Tuple::SetValue(const std::string &field_name, 
                     const Constant& val,
                     const Layout &layout) {
    // we should not implement varchar in setvalue
    // because we should consider about changing tuple's size
    // for varchar type, we use constructor get a new tuple
    SIMPLEDB_ASSERT(layout.GetSchema().GetType(field_name)
                    == TypeID::VARCHAR, "not implement");
    
    Schema schema = layout.GetSchema();
    int offset = layout.GetOffset(field_name);
    data_->SetValue(offset, val);
}

Constant Tuple::GetValue(const std::string &field_name,
                         const Layout &layout) {

    Schema schema = layout.GetSchema();
    int offset = layout.GetOffset(field_name);
    auto type = schema.GetType(field_name);

    switch (type)
    {
    case TypeID::INVALID:
        break;
    
    case TypeID::CHAR:
    case TypeID::INTEGER:
    case TypeID::DECIMAL:
        return Constant(data_->GetValue(offset, type));
        break;
    
    case TypeID::VARCHAR: {
        int data_offset = data_->GetInt(offset);
        SIMPLEDB_ASSERT(data_offset >= 0, "index should pass than 0");
        return Constant(data_->GetValue(data_offset, type));
        break;
        }
        
    default:
        break;
    }

    SIMPLEDB_ASSERT(false, "should not happen");
    return Constant();
}


std::string Tuple::ToString(const Layout &layout) {
    auto schema = layout.GetSchema();
    std::stringstream s;
    s << "{";
    for (auto t:schema.GetFields()) {
        s << "[field = " << t << ","
          << "offset = " << layout.GetOffset(t) << ","
          << "type = " << schema.GetType(t) << ","
          << "length = " << schema.GetLength(t) << ","
          << "data = " << GetValue(t, layout).to_string() << "], ";
    }
    s << "}";
    return s.str();
}



} // namespace SimpleDB

#endif