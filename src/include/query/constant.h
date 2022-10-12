#ifndef CONSTANT_H
#define CONSTANT_H

#include "config/macro.h"

#include <string>
#include <memory>

namespace SimpleDB {

/**
* The class that denotes values stored in the database.
* can we use template to replace it?
* TODO: 
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
    }

    Constant(int ival) {
        ival_ = std::make_unique<int>(ival);
    }

    Constant(std::string sval) {
        sval_ = std::make_unique<std::string>(sval);
    }

    int AsInt() const { 
        SIMPLEDB_ASSERT(ival_, "not exist");
        return *ival_; 
    }

    std::string AsString() const { 
        SIMPLEDB_ASSERT(sval_, "not exist");
        return *sval_; 
    }

    int HashCode() const {
        return (*ival_ ? std::hash<int>{}(*ival_) : std::hash<std::string>{}(*sval_));
    }
    
    bool IsNull() const {
        return !ival_ && !sval_;
    }

    std::string ToString() const {
        return ival_ ? std::to_string(*ival_) : *sval_;
    }

    bool operator <(const Constant &obj) const {
        return ival_ ? (*ival_ < *obj.ival_) :
                       (*sval_ < *obj.sval_);
    }

    bool operator >(const Constant &obj) const {
        return ival_ ? (*ival_ > *obj.ival_) :
                       (*sval_ > *obj.sval_);
    }

    Constant& operator =(const Constant &obj) {
        if (this != &obj) {
            *ival_ = *obj.ival_;
            *sval_ = *obj.sval_;
        }
        return *this;
    }

    bool operator <=(const Constant &obj) const {
        return *this == obj || *this < obj;
    }

    bool operator >=(const Constant &obj) const {
        return *this == obj || *this > obj; 
    }

    bool operator !=(const Constant &obj) const {        
        return ival_ ? *(ival_) != *(obj.ival_) :
                       *(sval_) != *(obj.sval_);
    }

    bool operator ==(const Constant &obj) const {
        return ival_ ? *(ival_) == *(obj.ival_)
                     : *(sval_) == *(obj.sval_);
    }


private:

    std::unique_ptr<int> ival_;

    std::unique_ptr<std::string> sval_;

};


} // namespace SimpleDB

#endif