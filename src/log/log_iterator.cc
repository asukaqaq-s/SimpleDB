#ifndef LOG_ITERATOR_CC
#define LOG_ITERATOR_CC

#include "log/log_iterator.h"
#include "config/macro.h"

#include <iostream>

namespace SimpleDB {

LogIterator::LogIterator(FileManager *file_manager, std::string file_name, int offset)
            : file_manager_(file_manager), log_name_(file_name), file_offset_(offset) {
    std::shared_ptr<std::vector<char>> array = 
            std::make_shared<std::vector<char>>(file_manager->BlockSize());
    read_buf_ = std::make_unique<Page> (array);
    buffer_offset_ = 0;
    buffer_size_ = file_manager_->BlockSize();
    MoveToRecord(file_offset_);
    log_file_size_ = file_manager_->GetFileSize(log_name_);
}

bool LogIterator::HasNextRecord() {
    
    if (file_offset_ < log_file_size_) {
        return true;
    }
    // cache to reduce io cost
    log_file_size_ = file_manager_->GetFileSize(log_name_);
    if (file_offset_ < log_file_size_) 
        return true;
    return false;
}

void LogIterator::NextRecord() {
    int add_len;
    
    if (!HasNextRecord()) {
        std::cerr << "next record not exist" << std::endl;
        return;
    } 

    add_len = read_buf_->GetInt(buffer_offset_) + sizeof(int); 
    if (add_len + buffer_offset_ > buffer_size_) {
        MoveToRecord(file_offset_);    
    }

    file_offset_ += add_len;
    buffer_offset_ += add_len;    
}

std::vector<char> LogIterator::MoveToRecord(int offset) {
    file_manager_->ReadLog(log_name_, offset, *read_buf_);
    buffer_offset_ = 0;
    file_offset_ = offset;
    
    auto log_record_vector = read_buf_->GetBytes(buffer_offset_);
    return log_record_vector;
}

std::vector<char> LogIterator::CurrentRecord() {
    int record_size;
    int need_size;
    
    // special case
    // the size of free space less than sizeof(int) 
    if(buffer_offset_ + sizeof(int) > buffer_size_) {
        MoveToRecord(file_offset_);
    }
    
    // normal case
    // a record spanned two pages
    record_size = read_buf_->GetInt(buffer_offset_);
    need_size = record_size + sizeof(int);
    if (need_size + buffer_offset_ > buffer_size_) {
        MoveToRecord(file_offset_);
    }

    auto log_record_vector = read_buf_->GetBytes(buffer_offset_);
    return log_record_vector;
}

} // namespace SimpleDB

#endif