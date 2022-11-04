#ifndef TYPE_ID_H
#define TYPE_ID_H

namespace SimpleDB {

// i think we just need to support this four type
// another type is based on these types
// such as timestamp can be stored as a integer
// datetime can be stored as a string
enum TypeID {

    INVALID = 0,
    INTEGER,
    DECIMAL,
    CHAR,
    VARCHAR,
};

// varchar: The varchar type only stores the specific size, 
//          but does not exceed the maxsize
// char: The char type will only store the maximum size of space


} // namespace SimpleDB

#endif