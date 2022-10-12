#ifndef SCAN_H
#define SCAN_H

#include "query/constant.h"

namespace SimpleDB {

/**
 * The interface will be implemented by each query scan.
 * There is a Scan class for each relational
 * algebra operator.
 */
class Scan {

public: 

    virtual ~Scan() = default;
    
    virtual void FirstTuple() = 0;
    
    virtual bool Next() = 0;
    
    virtual int GetInt(const std::string &filed_name) = 0;

    virtual std::string GetString(const std::string &field_name) = 0;

    virtual Constant GetVal(const std::string &field_name) = 0;

    virtual bool HasField(const std::string &field_name) = 0;
    
    virtual void Close() = 0;
}; 

} // namespace SimpleDB

#endif