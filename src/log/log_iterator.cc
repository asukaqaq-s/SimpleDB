#ifndef LOG_ITERATOR_CC
#define LOG_ITERATOR_CC

#include "log/log_iterator.h"
#include "config/macro.h"

namespace SimpleDB {

bool LogIterator::HasNextRecord() {
    if(block_.BlockNum() < log_file_size_)
        return true;
    else {
        log_file_size_ = file_manager_->Length(block_.FileName());
        return block_.BlockNum() < log_file_size_ ||
                current_pos_ < boundary_;
    }
    return false;
}

std::vector<char> LogIterator::NextRecord() {
    int add_len;
    std::vector<char> log_record;
    
    if(!HasNextRecord()) { // no more records
        return std::vector<char>(0);
    }
    if(current_pos_ == boundary_){ // should move to next block
        // update to new blockid
        block_ = BlockId(block_.FileName(), block_.BlockNum() + 1); 
        MoveToBlock(block_);
    }
    add_len = page_->GetInt(current_pos_) + sizeof(int);
    log_record = page_->GetBytes(current_pos_);
    // update the position of the iterator in the block
    current_pos_ += add_len;
    return log_record;
}

void LogIterator::MoveToBlock(BlockId block) {
    SIMPLEDB_ASSERT(block == block_, "LogIterator error"); /* for debugging purpose */

    file_manager_->Read(block, *page_);
    boundary_ = page_->GetInt(0);
    current_pos_ = sizeof(int);
}

std::vector<char> LogIterator::MoveToRecord(int offset) {
    int block_size = file_manager_->BlockSize();
    int block_number = offset / block_size;
    int block_offset = offset % block_size;
    std::vector<char> log_record;

    SIMPLEDB_ASSERT(block_number <= block_.BlockNum(),
                    "LogIterator error");
    if(block_number < block_.BlockNum()) {
        // need to move block
        MoveToBlock(BlockId(block_.FileName(), block_number));
    }
    SIMPLEDB_ASSERT(block_offset <= boundary_, "LogIterator error");
    
    current_pos_ = block_offset;
    log_record = page_->GetBytes(current_pos_);
    return log_record;
}

} // namespace SimpleDB

#endif