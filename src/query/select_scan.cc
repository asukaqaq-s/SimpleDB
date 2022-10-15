#ifndef SELECT_SCAN_CC
#define SELECT_SCAN_CC

#include "query/select_scan.h"

namespace SimpleDB {

SelectScan::SelectScan(const std::shared_ptr<Scan> &scan, 
    const Predicate &pred) : scan_(scan), pred_(pred) {
    FirstTuple();
}

// implemented scan 

void SelectScan::FirstTuple() {
    scan_->FirstTuple();
}

bool SelectScan::Next() {
    // the job of this method is to establish a new
    // current record. looking for a record that 
    // satisfies the predicate.
    while (scan_->Next()) {
        // search the tuple which satified all terms 
        // in predicate at the same time 
        if (pred_.IsSatisfied(scan_.get()))
            return true;
    }
    return false;
}
    
int SelectScan::GetInt(const std::string &field_name) {
    return scan_->GetInt(field_name);
}

std::string SelectScan::GetString
(const std::string &field_name) {
    return scan_->GetString(field_name);
}

Constant SelectScan::GetVal(const std::string &field_name) {
    return scan_->GetVal(field_name);
}
    
bool SelectScan::HasField(const std::string &field_name) {
    return scan_->HasField(field_name);
}

void SelectScan::Close() {
    scan_->Close();
}

// implemented updatescan

void SelectScan::SetInt(
    const std::string &field_name, 
    int val) {
    // for s2’s setString method will cast its underlying scan 
    // (i.e., s1) to an updatescan; if that scan is not updatable,
    // we should abort
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    ud_scan->SetInt(field_name, val);
}

void SelectScan::SetString(
    const std::string &field_name, 
    const std::string &val) {
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    ud_scan->SetString(field_name, val);
}

void SelectScan::SetVal(
    const std::string &field_name,
    const Constant &val) {
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    ud_scan->SetVal(field_name, val);
}
    
void SelectScan::Insert() {
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    ud_scan->Insert();
}
    
void SelectScan::Remove() {
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    
    ud_scan->Remove();
}

RID SelectScan::GetRid() {
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    
    return ud_scan->GetRid();
}

void SelectScan::MoveToRid(const RID &rid) {
    std::shared_ptr<UpdateScan> ud_scan = std::dynamic_pointer_cast<UpdateScan>(scan_);
    
    if (ud_scan == nullptr) {
        SIMPLEDB_ASSERT(false, "dynamic cast error");
    }
    
    ud_scan->MoveToRid(rid);
}

} // namespace SimpleDB

#endif
