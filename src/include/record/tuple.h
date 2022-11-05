#ifndef TUPLE_H
#define TUPLE_H

#include "record/schema.h"
#include "record/rid.h"
#include "type/value.h"

#include <cstring>

namespace SimpleDB {

/**
* @brief
* a tuple is a record which stored in database.
* description of single tuple that stays in memory
* Tuple format:
* -------------------------------
* | Schema  | varlen type data  |
* -------------------------------
* for the start of tuple, we will store a schema object in byte array.
* 
* for varlen type, in "fixlen type data", we just store the offset of the 
*     field in tuple, the real data will be stored in the end of tuple. 
* 
* i.e. for every columns, either it contains the corresponding fixed-size 
* value which can be retrieved based on column-offset in schema, or it 
* contains the offset of varied-size type, and the corresponding data 
* is placed at the end of the tuple.
*/
class Tuple {
    
public:
    
    Tuple() = default;

    Tuple(std::vector<char> data);

    Tuple(std::vector<Value> values, const Schema &schema);

    ~Tuple() = default;

    Tuple(const Tuple &other);
    Tuple& operator=(Tuple other);
    
    
    void Swap(Tuple &rhs) {
        std::swap(rhs.data_, data_);
        std::swap(rhs.size_, size_);
        std::swap(rid_, rhs.rid_);
    }


    bool operator ==(const Tuple &rhs) const {
        if (rhs.data_ != nullptr && 
            data_ != nullptr &&
            size_ == rhs.size_) {
            return memcmp(GetDataPtr(), rhs.GetDataPtr(), size_) == 0;
        }
        return false;
    }


    inline RID GetRID() const {
        return rid_;
    }

    inline void SetRID(const RID &rid) {
        rid_ = rid;
    }
    
    inline std::shared_ptr<std::vector<char>> GetData() const {
        return data_->content();
    }

    inline char* GetDataPtr() const {
        auto ptr = GetData();
        return &((*ptr)[0]);
    }

    /**
    * @brief Get the tuple size, including varlen object
    * @return int
    */
    inline int GetSize() const {
        return size_;
    }

    /**
    * @brief 
    * check whether the tuple contains data.
    * default tuple will be an invalid tuple.
    * @return true tuple is valid
    * @return false tuple doesn't contains any data
    */
    inline bool IsValid() const {
        return data_ != nullptr;
    }
    
    /**
    * @brief write a value to tuple
    * 
    * @param field_name
    * @param val
    * @param layout
    */
    void SetValue(const std::string &field_name, 
                  const Value& val,
                  const Schema &layout);

    /**
    * @brief read a value from tuple
    * 
    * @param field_name
    * @param val
    * @param layout
    */
    Value GetValue(const std::string &field_name,
                   const Schema &layout) const;


    std::string ToString() {
        auto array = *data_->content();
        return std::string(array.begin(), array.end());
    }

    /**
    * @brief a more convenient way to get tuple data
    */
    std::string ToString(const Schema &layout);

private:
    
    RID rid_;

    // total size of this tuple
    int size_{0};

    // payload
    std::unique_ptr<Page> data_;
    
};

} // namespace SimpleDB


#endif