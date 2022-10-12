#ifndef PRODUCT_SCAN_CC
#define PRODUCT_SCAN_CC

#include "query/product_scan.h"
#include "config/macro.h"

#include <iostream>

namespace SimpleDB {

ProductScan::ProductScan(const std::shared_ptr<Scan> &scan1,
                         const std::shared_ptr<Scan> &scan2) 
    : scan1_(scan1), scan2_(scan2) {
    
    FirstTuple();
}

void ProductScan::FirstTuple() {
    scan1_->FirstTuple();
    scan1_->Next();
    scan2_->FirstTuple();
}

bool ProductScan::Next() {
    // if we can move to the next RHS record
    // when we return: Scan1: LHS     Scan2: RHS + 1
    // if we can not move it
    // when we return: Scan1: LHS + 1 Scan2: 1
    if (scan2_->Next()) {
        return true;
    } else {
        scan2_->FirstTuple();
        return scan2_->Next() && scan1_->Next();
    }
    return false;
}

int ProductScan::GetInt(const std::string &field_name) {
    // check to see if the specified field is
    // in scan1. If so, then it accessed the field
    // using s1;otherwise, it accesses the field using s2.
    if (scan1_->HasField(field_name)) {
        return scan1_->GetInt(field_name);
    } else {
        return scan2_->GetInt(field_name);
    }

    SIMPLEDB_ASSERT(false, "should not happen ");
}

std::string ProductScan::GetString(
    const std::string &field_name) {
    
    if (scan1_->HasField(field_name)) {
        return scan1_->GetString(field_name);
    } else {
        return scan2_->GetString(field_name);
    }

    SIMPLEDB_ASSERT(false, "should not happen");
}

Constant ProductScan::GetVal(const std::string &field_name) {
    
    if (scan1_->HasField(field_name)) {
        return scan1_->GetVal(field_name);
    } else {
        return scan2_->GetVal(field_name);
    }

    SIMPLEDB_ASSERT(false, "should not happen ");
}

bool ProductScan::HasField(const std::string &field_name) {
    return scan1_->HasField(field_name) ||
           scan2_->HasField(field_name);
}

void ProductScan::Close() {
    scan1_->Close();
    scan2_->Close();
}

} // namespace SimpleDB

#endif
