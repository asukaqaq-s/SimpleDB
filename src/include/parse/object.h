#ifndef OBJECT_H
#define OBJECT_H

#include <fstream>
#include <sstream>

namespace SimpleDB {

/**
* @brief a father Object
*/
class Object {

public: 

    enum Operation {

        INSERT = 0,
        REMOVE,
        MODIFY,
        CREATETABLE,
        CREATEVIEW,
        CREATEINDEX
    };

    virtual ~Object() = default;

    virtual int GetOP() = 0;

    virtual std::string ToString() = 0;
};

inline std::ostream &operator<<(std::ostream &os, const Object::Operation &op) {
    
    switch (op) {
    case Object::INSERT:
        os << "insert";
        break;
    case Object::REMOVE:
        os << "remove";
        break;
    case Object::MODIFY:
        os << "modify";
        break;
    case Object::CREATETABLE:
        os << "createtable";
        break;
    case Object::CREATEVIEW:
        os << "createview";
        break;
    case Object::CREATEINDEX:
        os << "createindex";
        break;
    default:
        break;
    }
  
    return os;       
}


} // namespace SimpleDB

#endif