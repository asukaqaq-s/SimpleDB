#ifndef LOG_MANAGER_CC
#define LOG_MANAGER_CC

#include "log/log_manager.h"

#include <vector>
#include <iostream>

namespace SimpleDB {

LogManager::LogManager(FileManager* file_manager, std::string log_file_name) :
    file_manager_(file_manager), log_file_name_(log_file_name) , buffer_size_(file_manager_->BlockSize() * 10){

    std::shared_ptr<std::vector<char>> 
        array = std::make_shared<std::vector<char>>(buffer_size_, 0);

    // the logical block number of log file
    log_buffer_ = std::make_unique<Page>(array); 
    // the size of log in the current memory
    log_count_ = 0;
    log_file_size_ = file_manager_->GetFileSize(log_file_name_);
}

void LogManager::Flush() {
    
    for(auto t : *(*log_buffer_).content()) {
        // std::cout << t << std::flush;
    }
    
    file_manager_->WriteLog(log_file_name_, log_count_, *log_buffer_);
    // clear log_count_ after writelog
    log_count_ = 0;
    last_flush_lsn_.store(lastest_lsn_); 
}

void LogManager::Flush(int lsn) {
    if(lsn >= last_flush_lsn_ ) {
        Flush();
    }
}

// note that the size of a log record which stores in disk is equal
// to (log_record_length + sizeof(int))
lsn_t LogManager::Append(const std::vector<char> &log_record) {
    std::lock_guard<std::mutex> lock(latch_);
    int record_length = log_record.size(); 
    // The starting location where the page is stored
    int start_address = log_count_;
    // the size of log record  + sizeof(int)
    int need_size = Page::MaxLength(record_length); 

    if (start_address + need_size > buffer_size_) { // past than a buffer
        // sizeof(int) means the size of the page-header
        Flush();
        start_address = 0;
    }
    
    log_buffer_->SetBytes(start_address, log_record);
    // now, the lastestlsn corresponds to the current log
    lastest_lsn_ ++;
    // update log file size and log_count_
    log_file_size_ += need_size;
    log_count_ += need_size;

    return lastest_lsn_.load(); 
}

lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
    std::lock_guard<std::mutex> lock(latch_);
    int record_length = log_record.RecordSize(); 
    int start_address = log_count_; 
    int need_size = Page::MaxLength(record_length); 
    
    if (start_address + need_size > buffer_size_) {
        // sizeof(int) means the size of the page-header
        Flush();
        start_address = 0;
    }
    
    lastest_lsn_ ++; 
    // now, The lastestlsn corresponds to the current log
    log_record.SetLsn(lastest_lsn_); 
    auto log_record_vector = log_record.Serializeto();
    log_buffer_->SetBytes(start_address, *log_record_vector);
    // update log file size and log_count_
    log_file_size_ += need_size;
    log_count_ += need_size;

    return lastest_lsn_.load(); /* return the lsn of current log */
}

lsn_t LogManager::AppendLogWithOffset(LogRecord &log_record,int *offset) {
    std::lock_guard<std::mutex> lock(latch_);
    int record_length = log_record.RecordSize(); 
    int start_address = log_count_; 
    int need_size = Page::MaxLength(record_length); 

    if(start_address + need_size > buffer_size_) {
        // sizeof(int) means the size of the page-header
        Flush();
        start_address = 0;
    }
    
    lastest_lsn_ ++; 
    // now, The lastestlsn corresponds to the current log
    log_record.SetLsn(lastest_lsn_);
    auto log_record_vector = log_record.Serializeto();
    log_buffer_->SetBytes(start_address, *log_record_vector);
    *offset = log_file_size_;
    // update log file size and log_count_
    log_file_size_ += need_size;
    log_count_ += need_size;

    return lastest_lsn_.load(); /* return the lsn of current log */
}



LogIterator LogManager::Iterator() {
    Flush(); /* because we will access the log file 
            which stored in disk, so should flush it */

    // return the first log stores in log file
    return LogIterator(file_manager_, log_file_name_, 0);
}


LogIterator LogManager::Iterator(int offset) {
    Flush();
    
    // return the specifed log stores in log file
    return LogIterator(file_manager_, log_file_name_, offset);
}

} // namespace SimpleDB

#endif