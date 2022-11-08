#ifndef STATEMENT_H
#define STATEMENT_H

#include "config/macro.h"

#include <fstream>
#include <sstream>


namespace SimpleDB {


enum class StatementType : uint8_t {
    SELECT = 0,
    INSERT,
    DELETE,
    UPDATE,
    CREATETABLE,
    CREATEVIEW,
    CREATEINDEX
};



/**
* @brief the base Statement class
*/
class Statement {

public: 

    virtual ~Statement() = default;

    virtual StatementType GetStmtType() = 0;

    virtual std::string ToString() = 0;

    
};


inline std::ostream &operator<<(std::ostream &os, const StatementType &op) {
    
    switch (op) {

    case StatementType::SELECT:
        os << "select";
        break;
    case StatementType::INSERT:
        os << "insert";
        break;
    case StatementType::DELETE:
        os << "delete";
        break;
    case StatementType::UPDATE:
        os << "update";
        break;
    case StatementType::CREATETABLE:
        os << "create table";
        break;
    case StatementType::CREATEVIEW:
        os << "create view";
        break;
    case StatementType::CREATEINDEX:
        os << "create index";
        break;
    
    default:
        SIMPLEDB_ASSERT(false, "can't reach");
        break;
    }
  
    return os;       
}


} // namespace SimpleDB




#endif