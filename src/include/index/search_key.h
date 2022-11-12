#ifndef SEARCH_KEY_H
#define SEARCH_KEY_H

#include "type/value.h"
#include "record/schema.h"
#include "record/tuple.h"

#include <vector>

namespace SimpleDB {

class SearchKey {

public:

    SearchKey(const Value &value, Schema *key_schema) 
        : key_schema_(key_schema) {
        SIMPLEDB_ASSERT(value.GetTypeID() == key_schema->GetColumn(0).GetType(),
                        "type not match");
        key_size_ = DeduceKeySize(key_schema);
        values_.emplace_back(GenerateValue(value, 0));
    }


    SearchKey(const std::vector<Value> &values, Schema *key_schema) 
        : key_schema_(key_schema) {

        key_size_ = DeduceKeySize(key_schema);
        int size = values.size();
        for (int i = 0;i < size; i++) {
            SIMPLEDB_ASSERT(values[i].GetTypeID() == key_schema->GetColumn(i).GetType(),
                            "type not match");
            values_.emplace_back(GenerateValue(values[i], i));
        }
    }


    SearchKey(const Tuple &tuple, Schema *key_schema)
        : key_schema_(key_schema) {

        int column_count = key_schema_->GetColumnsCount();
        for (int i = 0;i < column_count; i++) {
            auto column_name = key_schema_->GetColumn(i).GetName();
            values_.emplace_back(GenerateValue(
                tuple.GetValue(column_name, *key_schema_), i));
        }
    }


    Tuple SerializationKey() const {
        return Tuple(values_, *key_schema_);   
    }

    
    Schema* GetKeySchema() {
        return key_schema_;
    }


    std::string ToString() const {
        std::stringstream s;
        for (auto &t:values_) {
            s << t.to_string();
        }
        return s.str();
    }


    bool operator == (const SearchKey &other) {
        return values_ == other.values_;
    }

    bool operator <= (const SearchKey &other) {
        return values_ <= other.values_;
    }

    bool operator < (const SearchKey &other) {
        return values_ < other.values_;
    }    

    bool operator >= (const SearchKey &other) {
        return values_ >= other.values_;
    }

    bool operator > (const SearchKey &other) {
        return values_ > other.values_;
    }

    bool operator != (const SearchKey &other) {
        return values_ != other.values_;
    }


    static int DeduceKeySize(const Schema *key_schema) {
        int key_size = 0;

        for (const auto &t: key_schema->GetColumns()) {
            if (t.GetType() == TypeID::VARCHAR ||
                t.GetType() == TypeID::CHAR) {
                key_size += Page::MaxLength(t.GetLength());
            } 
            else {
                key_size += t.GetLength();
            }
        }

        return key_size;
    }



private:


    /**
    * @brief For convenience, converting varchar to maxlength 
    * ensures that the tuple is fixed-size
    */
    Value GenerateValue(const Value &value, int column_idx) {
        if (value.GetDataType() == DataType::STRING) {
            auto str = value.AsString();
            int str_max_size = key_schema_->GetColumn(column_idx).GetLength();

            str.resize(str_max_size);
            return Value(str, value.GetTypeID());
        }

        return value;
    }


    std::vector<Value> values_;

    Schema* key_schema_{nullptr};

    // cache key size
    int key_size_{0};

};

} // namespace SimpleDB


#endif