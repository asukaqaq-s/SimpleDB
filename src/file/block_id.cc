#ifndef BLOCK_ID_CC
#define BLOCK_ID_CC

#include "file/block_id.h"

namespace SimpleDB {

bool operator == (const BlockId &lobj, const BlockId &robj) {
    return (lobj.block_num_ == robj.block_num_) &&
           (lobj.file_name_ == robj.file_name_);
}

bool operator != (const BlockId &lobj, const BlockId &robj) {
    if(lobj == robj) {
        return true;
    } else {
        return false;
    }
}

bool operator < (const BlockId &lobj, const BlockId &robj) {
    if(lobj.file_name_ == robj.file_name_) {
        return lobj.block_num_ < robj.block_num_;
    } else if(lobj.block_num_ == robj.block_num_) {
        return lobj.file_name_ < robj.file_name_;
    }
    return false;
}

bool operator > (const BlockId &lobj, const BlockId &robj) {
    if(lobj.file_name_ == robj.file_name_) {
        return lobj.block_num_ > robj.block_num_;
    } else if(robj.block_num_ == lobj.block_num_) {
        return lobj.file_name_ > robj.file_name_;
    }
    return false;
}

bool operator <= (const BlockId &lobj, const BlockId &robj) {
    if(lobj > robj) {
        return false;
    } else {
        return true;
    }
}

bool operator >= (const BlockId &lobj, const BlockId &robj) {
    if(lobj < robj) {
        return false;
    } else {
        return true;
    }
}

BlockId& BlockId::operator= (const BlockId &obj) {
    block_num_ = obj.block_num_;
    file_name_ = obj.file_name_;
    return *this;
}

std::string BlockId::FileName() const {
    return file_name_;    
}

int BlockId::BlockNum() const {
    return block_num_;
}

bool BlockId::equals(const BlockId &obj) const {
    return obj == *this;
}

std::string BlockId::to_string() const {
    std::string temp = "filename = " + file_name_ + ","
                       "blocknum = " + std::to_string(block_num_);
    return temp;
}

}
#endif