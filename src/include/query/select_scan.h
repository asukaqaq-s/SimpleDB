#ifndef SELECT_SCAN_H
#define SELECT_SCAN_H

#include "query/update_scan.h"
#include "query/predicate.h"
#include "record/rid.h"

namespace SimpleDB {

/**
* @brief The scan class corresponding the select relational
* algebra operator
* All methods except next delegate their work the underlying scan
*/
class SelectScan : public UpdateScan {

public:

    // SelectScan() = default;

    SelectScan(const std::shared_ptr<Scan> &scan, const Predicate &pred);

// implemented scan 

    void FirstTuple() override;

    bool Next() override;
    
    int GetInt(const std::string &field_name) override;

    std::string GetString(const std::string &field_name) override;

    Constant GetVal(const std::string &field_name) override;
    
    bool HasField(const std::string &field_name) override;

    void Close() override;

// implemented updatescan

    void SetInt(const std::string &field_name, int val) override;

    void SetString(const std::string &field_name, 
                   const std::string &val) override;

    void SetVal(const std::string &field_name,
                const Constant &val) override;
    
    void Insert() override;
    
    void Remove() override;

    RID GetRid() override;

    void MoveToRid(const RID &rid) override;

private:
    
    std::shared_ptr<Scan> scan_;

    Predicate pred_;
};

} // namespace SimpleDB

#endif