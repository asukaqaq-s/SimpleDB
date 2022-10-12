#ifndef UPDATE_SCAN_H
#define UPDATE_SCAN_H

#include "query/scan.h"
#include "record/rid.h"

namespace SimpleDB {

/**
* @brief the interface implemented by all update scans
* we can 
*/
class UpdateScan : public Scan {

public:
    
    
    virtual ~UpdateScan() = default;

    virtual void SetInt(const std::string &field_name, int val) = 0;

    virtual void SetString(const std::string &field_name,
                           const std::string &pval) = 0;
    
    virtual void SetVal(const std::string &field_name,
                        const Constant &val) = 0;
    
    virtual void Insert() = 0;
    
    virtual void Remove() = 0;

    virtual RID GetRid() = 0;

    virtual void MoveToRid(const RID &rid) = 0;
};

} // namespace SimpleDB

#endif
