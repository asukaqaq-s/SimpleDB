#ifndef CONSTANT_H
#define CONSTANT_H

#include "config/macro.h"
#include "type/typeid.h"
#include <memory>

#include <string>

namespace SimpleDB {

enum DataType {
    INT,
    REAL,
    STRING
};

/**
* @brief The class that denotes values stored in the database.
*/
class Value {

public:

    Value() {}

    /**
    * @brief in this constructor, we just store typeid
    * and actual value is null
    */
    Value(TypeID type) : type_id_(type) {}
    
    Value(const Value & obj) {
        if (obj.ival_) {
            ival_ = std::make_unique<int> (*(obj.ival_));
        }
        if (obj.sval_) {
            sval_ = std::make_unique<std::string> (*(obj.sval_));
        }
        if (obj.dval_) {
            dval_ = std::make_unique<double> (*(obj.dval_));
        }
        type_id_ = obj.type_id_;
    }

    Value(int ival) {
        ival_ = std::make_unique<int>(ival);
        type_id_ = TypeID::INTEGER;
    }

    Value(std::string sval, TypeID type_id) {
        sval_ = std::make_unique<std::string>(sval);
        type_id_ = type_id;
    }

    Value(double dval) {
        dval_ = std::make_unique<double> (dval);
        type_id_ = TypeID::DECIMAL;
    }

    bool IsTrue() const {
        SIMPLEDB_ASSERT(ival_, "not exist");
        return *ival_ == true;
    }

    bool IsFalse() const {
        SIMPLEDB_ASSERT(ival_, "not exist");
        return *ival_ == false;
    }

    int AsInt() const { 
        SIMPLEDB_ASSERT(ival_, "not exist");
        return *ival_; 
    }

    std::string AsString() const { 
        SIMPLEDB_ASSERT(sval_, "not exist");
        return *sval_; 
    }

    double AsDouble() const {
        SIMPLEDB_ASSERT(dval_, "not exist");
        return *dval_;
    }

    TypeID GetTypeID() const {
        return type_id_;
    }

    DataType GetDataType() const {
        if (ival_) {
            return DataType::INT;
        }
        if (dval_) {
            return DataType::REAL;
        }
        if (sval_) {
            return DataType::STRING;
        }

        SIMPLEDB_ASSERT(false, "");
    }

    int GetLength() const {
        switch(type_id_) {
        
        case TypeID::INTEGER:
            return sizeof(int);

        case TypeID::DECIMAL:
            return sizeof(double);
        
        case TypeID::CHAR:
        case TypeID::VARCHAR:
            return sval_->size();
        
        default:
            break;
        }

        SIMPLEDB_ASSERT(false, "can't reach it");
        return 0;
    }


    int hash_code() const {
        if (ival_) {
            return std::hash<int> {}(*ival_);
        }
        if (sval_) {
            return std::hash<std::string> {}(*sval_);
        }
        if (dval_) {
            return std::hash<double> {}(*dval_);
        }
        return 0;
    }
    
    std::string to_string() const {
        if (ival_) {
            return std::to_string(*ival_);
        }
        if (dval_) {
            return std::to_string(*dval_);
        }
        if (sval_) {
            return *sval_;
        }
        return "";
    }

    bool IsNull() const {
        return !ival_ && !sval_ && !dval_;
    }

    bool operator <(const Value &obj) const {
        if (ival_) {
            return *ival_ < *obj.ival_;
        } 
        if (sval_) {
            return *sval_ < *obj.sval_;
        }
        if (dval_) {
            return *dval_ < *obj.dval_;
        }
        return false;
    }

    bool operator >(const Value &obj) const {
        if (ival_) {
            return *ival_ > *obj.ival_;
        } 
        if (sval_) {
            return *sval_ > *obj.sval_;
        }
        if (dval_) {
            return *dval_ > *obj.dval_;
        }
        return false;
    }

    Value& operator =(const Value &obj) {
        if (this != &obj) {
            if (ival_) {
                *ival_ = *obj.ival_;
            }
            if (sval_) {
                *sval_ = *obj.sval_;
            }
            if (dval_) {
                *dval_ = *obj.dval_;
            }
            
        }
        return *this;
    }

    bool operator <=(const Value &obj) const {
        if (ival_) {
            return *ival_ <= *obj.ival_;
        } 
        if (sval_) {
            return *sval_ <= *obj.sval_;
        }
        if (dval_) {
            return *dval_ <= *obj.dval_;
        }
        return false;
    }

    bool operator >=(const Value &obj) const {
        if (ival_) {
            return *ival_ >= *obj.ival_;
        } 
        if (sval_) {
            return *sval_ >= *obj.sval_;
        }
        if (dval_) {
            return *dval_ >= *obj.dval_;
        }
        return false;
    }

    bool operator !=(const Value &obj) const {        
        if (ival_) {
            return *ival_ != *obj.ival_;
        } 
        if (sval_) {
            return *sval_ != *obj.sval_;
        }
        if (dval_) {
            return *dval_ != *obj.dval_;
        }
        return false;
    }

    bool operator ==(const Value &obj) const {
        if (ival_) {
            return *ival_ == *obj.ival_;
        } 
        if (sval_) {
            return *sval_ == *obj.sval_;
        }
        if (dval_) {
            return *dval_ == *obj.dval_;
        }
        return false;
    }

    Value operator +(const Value &obj) {
        if (ival_) {
            return Value(*ival_ + *obj.ival_);
        }
        if (sval_) {
            SIMPLEDB_ASSERT(false, "not implement");
        }
        if (dval_) {
            return Value(*dval_ + *obj.dval_);
        }
        return Value();
    }

    Value operator -(const Value &obj) {
        if (ival_) {
            return Value(*ival_ - *obj.ival_);
        }
        if (sval_) {
            SIMPLEDB_ASSERT(false, "not implement");
        }
        if (dval_) {
            return Value(*dval_ - *obj.dval_);
        }
        return Value();
    }

    Value operator *(const Value &obj) {
        if (ival_) {
            return Value(*ival_ * *obj.ival_);
        }
        if (sval_) {
            SIMPLEDB_ASSERT(false, "not implement");
        }
        if (dval_) {
            return Value(*dval_ * *obj.dval_);
        }
        return Value();
    }

    Value operator /(const Value &obj) {
        if (ival_) {
            assert(*obj.ival_ != 0);
            return Value(*ival_ / *obj.ival_);
        }
        if (sval_) {
            SIMPLEDB_ASSERT(false, "not implement");
        }
        if (dval_) {
            assert(*obj.dval_ != 0);
            return Value(*dval_ / *obj.dval_);
        }
        return Value();
    }
    
    Value operator %(const Value &obj) {
        if (ival_) {
            assert(*obj.ival_ != 0);
            return Value(*ival_ % *obj.ival_);
        }
        if (sval_) {
            SIMPLEDB_ASSERT(false, "not implement");
        }
        if (dval_) {
            SIMPLEDB_ASSERT(false, "not implement");
        }
        return Value();
    }
    



private:

    std::unique_ptr<int> ival_;

    std::unique_ptr<std::string> sval_;

    std::unique_ptr<double> dval_;

    TypeID type_id_;
};

} // namespace SimpleDB

#endif