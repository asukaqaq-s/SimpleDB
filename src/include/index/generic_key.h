#ifndef GENERIC_KEY_H
#define GENERIC_KEY_H

#include "record/tuple.h"
#include "record/column.h"
#include "record/schema.h"
#include "type/value.h"

#include <cassert>

namespace SimpleDB {

/**
 * @brief 
 * Generic key is used for indexing with opaque data.
 * This key type uses an fixed length array to hold data for indexing purposes,
 * the actual size of which is specified and instantiated with a template argument
 * @tparam KeySize 
 */
template <int KeySize>
class GenericKey {

public:

    inline void SetFromKey(const Tuple &tuple) {
        // initialize to all zero
        memset(data_, 0, KeySize);

        // if overflow, will discard it
        // if less than keysize, will padded by 0
        memcpy(data_, tuple.GetDataPtr(), std::min(tuple.GetSize(), KeySize));
    }


    Value ToValue(Schema *schema, uint32_t column_idx) const {
        auto col = schema->GetColumn(column_idx);
        if (col.IsInlined()) {
            return GetValueByBytes(data_ + col.GetOffset(), col.GetType(), col.GetLength());
        } else {
            uint32_t offset = *reinterpret_cast<const uint32_t *> (data_ + col.GetOffset());
            return GetValueByBytes(data_ + offset, col.GetType(), col.GetLength());
        }
    }

    inline const char *ToBytes() const {
        return data_;
    }

private:

    Value GetValueByBytes(const char *src, TypeID type, int length) const {
        switch (type)
        {
        case TypeID::INTEGER:
        {
            int value = *reinterpret_cast<const int*>(src);
            return Value(value);
        }
        case TypeID::DECIMAL:
        {
                double value = *reinterpret_cast<const double*>(src);
            return Value(value);
        }
        case TypeID::CHAR:
        {
            char buf[100];
            int size = *reinterpret_cast<const int*>(src);
            assert(size == length);

            std::memcpy(buf, src + sizeof(int), size);
            return Value(std::string(buf), type);
        }
        case TypeID::VARCHAR:
        {   
            char buf[100];
            
            std::memcpy(buf, src, length);
            return Value(std::string(buf), type);
        }
        default:
            break;
        }

        assert(false);
        return Value();
    }


    char data_[KeySize];
};


template<int KeySize> 
class GenericComparator {

public:
    
    explicit GenericComparator(Schema *key_schema) : key_schema_(key_schema) {}
    
    GenericComparator(const GenericComparator &other) : key_schema_(other.key_schema_) {}

    inline int operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const {
        uint32_t column_cnt = key_schema_->GetColumnsCount();
        for (uint32_t i = 0; i < column_cnt; i++) {
            Value lhs_value = lhs.ToValue(key_schema_, i);
            Value rhs_value = rhs.ToValue(key_schema_, i);

            if (lhs_value < rhs_value) {
                return -1;
            }

            if (lhs_value > rhs_value) {
                return 1;
            }

        }
        
        return 0;
    }

private:
    Schema *key_schema_;
};



}

#endif