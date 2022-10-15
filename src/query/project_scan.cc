#ifndef PROJECT_SCAN_CC
#define PROJECT_SCAN_CC

#include "query/project_scan.h"
#include "config/macro.h"

#include <algorithm>

namespace SimpleDB {

ProjectScan::ProjectScan(
    const std::shared_ptr<Scan> &scan, 
    const std::vector<std::string> &field_list)
    : scan_(scan), field_list_(field_list) {
    FirstTuple();
}

void ProjectScan::FirstTuple() {
    scan_->FirstTuple();
}

bool ProjectScan::Next() {
    return scan_->Next();
}

int ProjectScan::GetInt(const std::string &field_name) {
    if (!HasField(field_name)) {
        std::string cmd = "can not find field " + field_name;
        SIMPLEDB_ASSERT(false, cmd.c_str());
    }
    
    return scan_->GetInt(field_name);
}

std::string ProjectScan::GetString
    (const std::string &field_name) {
    if (!HasField(field_name)) {
        std::string cmd = "can not find field " + field_name;
        SIMPLEDB_ASSERT(false, cmd.c_str());
    }
    
    return scan_->GetString(field_name);
}

Constant ProjectScan::GetVal(const std::string &field_name) {
    if (!HasField(field_name)) {
        std::string cmd = "can not find field " + field_name;
        SIMPLEDB_ASSERT(false, cmd.c_str());
    }
    
    return scan_->GetVal(field_name);
}

bool ProjectScan::HasField(const std::string &field_name) {
    return  std::find(field_list_.begin(), 
                      field_list_.end(), 
                      field_name)
            != field_list_.end();

}

void ProjectScan::Close() {
    scan_->Close();
}

} // namespace SimpleDB 

#endif