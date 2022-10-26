#ifndef CONSTANT_H
#define CONSTANT_H

#include "config/macro.h"
#include "type/typeid.h"
#include <memory>

#include <string>

namespace SimpleDB {

/**
* @brief The class that denotes values stored in the 
* database.
* i change the location of this object for more convinent 
*/
class Constant {

public:

    Constant() {}

    Constant(const Constant & obj) {
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

    Constant(int ival) {
        ival_ = std::make_unique<int>(ival);
    }

    Constant(std::string sval) {
        sval_ = std::make_unique<std::string>(sval);
        type_id_ = TypeID::VARCHAR;
    }

    Constant(std::string sval, TypeID type_id) {
        sval_ = std::make_unique<std::string>(sval);
        type_id_ = type_id;
    }

    Constant(double dval) {
        dval_ = std::make_unique<double> (dval);
        type_id_ = TypeID::DECIMAL;
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

    bool operator <(const Constant &obj) const {
        if (ival_) {
            return *ival_ < *obj.ival_;
        } 
        if (sval_) {
            return *sval_ < *obj.sval_;
        }
        if (dval_) {
            return *dval_ < *obj.dval_;
        }
    }

    bool operator >(const Constant &obj) const {
        if (ival_) {
            return *ival_ > *obj.ival_;
        } 
        if (sval_) {
            return *sval_ > *obj.sval_;
        }
        if (dval_) {
            return *dval_ > *obj.dval_;
        }
    }

    Constant& operator =(const Constant &obj) {
        if (this != &obj) {
            *ival_ = *obj.ival_;
            *sval_ = *obj.sval_;
            *dval_ = *obj.dval_;
        }
        return *this;
    }

    bool operator <=(const Constant &obj) const {
        if (ival_) {
            return *ival_ <= *obj.ival_;
        } 
        if (sval_) {
            return *sval_ <= *obj.sval_;
        }
        if (dval_) {
            return *dval_ <= *obj.dval_;
        }
    }

    bool operator >=(const Constant &obj) const {
        if (ival_) {
            return *ival_ >= *obj.ival_;
        } 
        if (sval_) {
            return *sval_ >= *obj.sval_;
        }
        if (dval_) {
            return *dval_ >= *obj.dval_;
        }
    }

    bool operator !=(const Constant &obj) const {        
        if (ival_) {
            return *ival_ != *obj.ival_;
        } 
        if (sval_) {
            return *sval_ != *obj.sval_;
        }
        if (dval_) {
            return *dval_ != *obj.dval_;
        }
    }

    bool operator ==(const Constant &obj) const {
        if (ival_) {
            return *ival_ == *obj.ival_;
        } 
        if (sval_) {
            return *sval_ == *obj.sval_;
        }
        if (dval_) {
            return *dval_ == *obj.dval_;
        }
    }


private:

    std::unique_ptr<int> ival_;

    std::unique_ptr<std::string> sval_;

    std::unique_ptr<double> dval_;

    TypeID type_id_;
};

} // namespace SimpleDB

#endif