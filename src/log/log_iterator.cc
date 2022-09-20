#ifndef LOG_ITERATOR_CC
#define LOG_ITERATOR_CC

#include "log/log_iterator.h"

#include <assert.h>

namespace SimpleDB {

bool LogIterator::HasNextRecord() {
    int block_size = file_manager_->BlockSize();
    return current_pos_ < block_size ||
        block_.BlockNum() > 0;
}

std::vector<char> LogIterator::NextRecord() {
    int block_size = file_manager_->BlockSize();
    int add_len;
    std::vector<char> log_record;
    
    if(!HasNextRecord()) { // no more records
        return std::vector<char>(0);
    }
    if(current_pos_ == block_size){ // should move to last block
        // update to new blockid
        block_ = BlockId(block_.FileName(), block_.BlockNum() - 1); 
        MoveToBlock(block_);
    }
    add_len = page_->GetInt(current_pos_) + sizeof(int);
    log_record = page_->GetBytes(current_pos_);
    // update the position of the iterator in the block
    current_pos_ += add_len;
    return log_record;
}

void LogIterator::MoveToBlock(BlockId block) {
    assert(block == block_); /* for debugging purpose */

    file_manager_->Read(block, *page_);
    boundary_ = page_->GetInt(0);
    current_pos_ = boundary_;
}

} // namespace SimpleDB

#endif