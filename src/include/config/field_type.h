#ifndef FIELD_TYPE_H
#define FIELD_TYPE_H

#include "config/macro.h"

namespace SimpleDB {

enum FieldType {
    INVALID = 0,
    BOOLEAN = 1,
    TINYINT = 2,
    SMALLINT = 3,
    INTEGER = 4,
    CHAR = 5,
    BIGINT = 6,
    DECIMAL = 7,
    VARCHAR = 8,
    TIMESTAMP = 9,
};


class Type {

    static int GetTypeSize(FieldType type) {
        switch (type)
        {
        case FieldType::BOOLEAN:
            return static_cast<int>(sizeof(bool));
            break;

        case FieldType::TINYINT:
        case FieldType::SMALLINT:
            return static_cast<int>(sizeof(short));
            break;
        
        case FieldType::INTEGER:
            return static_cast<int>(sizeof(int));
            break;
        
        case FieldType::BIGINT:
        case FieldType::DECIMAL:
        case FieldType::TIMESTAMP:
            return static_cast<int>(sizeof(int));
            break;

        default:
            break;
        }
        
        SIMPLEDB_ASSERT(false, "should not happen");
        return -1;
    }

};


}; // namespace SimpleDB

#endif
