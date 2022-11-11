#ifndef SEARCH_KEY_H
#define SEARCH_KEY_H

#include "type/value.h"
#include "record/schema.h"

#include <vector>

namespace SimpleDB {

class SearchKey {

public:

    SearchKey(const Value &value, const Schema *key_schema) {
        SIMPLEDB_ASSERT(value.GetTypeID() == key_schema->GetColumn(0).GetType(),
                        "type not match");
        key_size_ = DeduceKeySize(key_schema);
        values_.emplace_back(GenerateKey(value));
    }

    Value GetValue() const {
        return values_[0];
    }

    std::string ToString() const {
        return values_[0].to_string();
    }


    static int DeduceKeySize(const Schema *key_schema) {
        auto type = key_schema->GetColumn(0).GetType();
        int key_size;
        if (type == TypeID::VARCHAR) {
            key_size = Page::MaxLength(key_schema->GetColumn(0).GetLength());
        } else {
            key_size = key_schema->GetLength();
        }

        return key_size;
    }

private:

    Value GenerateKey(const Value &value) {
        if (value.GetDataType() == DataType::STRING) {
            auto str = value.AsString();
            int payload_size = key_size_ - sizeof(int);
            str.resize(payload_size);
            
            return Value(str, value.GetTypeID());
        }

        return value;
    }


    std::vector<Value> values_;

    int key_size_{0};

};

} // namespace SimpleDB


#endif