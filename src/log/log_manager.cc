#ifndef LOG_MANAGER_CC
#define LOG_MANAGER_CC

#include "log/log_manager.h"

#include <vector>
#include <iostream>

namespace SimpleDB {

// if the log_file is empty, assign a new block to it
// otherwise, read the last block to memory
LogManager::LogManager(FileManager* file_manager, std::string log_file_name) :
    file_manager_(file_manager), log_file_name_(log_file_name) , block_size_(file_manager_->BlockSize()){
    int log_size = file_manager_->Length(log_file_name_); /* the next logical block number */
    std::shared_ptr<std::vector<char>> 
        array = std::make_shared<std::vector<char>>(block_size_, 0);

    log_page_ = std::make_unique<Page>(array); /* the logical block number of log file */
    
    if(log_size == 0) {
        current_block_ = AppendNewBlock();
    } else {
        current_block_ = BlockId(log_file_name_, log_size - 1);
        // read disk block into memory
        file_manager_->Read(current_block_, (*log_page_));
    }
}

BlockId LogManager::AppendNewBlock() {
    BlockId block = file_manager_->Append(log_file_name_); // assign a empty page
    log_page_->SetInt(0, sizeof(int));
    file_manager_->Write(block, *log_page_);
    return block;
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
    int start_address = log_page_->GetInt(0); 
    // the size of log record + sizeof(int)
    int need_size = Page::MaxLength(record_length); 

    if(start_address + need_size > block_size_ ) { // past than a page
        // sizeof(int) means the size of the page-header
        Flush();
        current_block_ = AppendNewBlock();
        start_address = log_page_->GetInt(0);
    }
    
    log_page_->SetBytes(start_address, log_record);
    log_page_->SetInt(0, start_address + need_size);
    lastest_lsn_ ++;  /* now, the lastestlsn corresponds to the current log */
    return lastest_lsn_.load(); 
}

lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
    std::lock_guard<std::mutex> lock(latch_);
    int record_length = log_record.RecordSize(); 
    int start_address = log_page_->GetInt(0); 
    int need_size = Page::MaxLength(record_length); 
    
    if(start_address + need_size > block_size_) {
        // sizeof(int) means the size of the page-header
        Flush();
        current_block_ = AppendNewBlock();
        start_address = log_page_->GetInt(0);
    }
    
    lastest_lsn_ ++; 
    log_record.SetLsn(lastest_lsn_); /* now, The lastestlsn corresponds to the current log */
    auto log_record_vector = log_record.Serializeto();
    log_page_->SetBytes(start_address, *log_record_vector);
    log_page_->SetInt(0, start_address + Page::MaxLength(record_length));
    return lastest_lsn_.load(); /* return the lsn of current log */
}

lsn_t LogManager::AppendLogWithOffset(LogRecord &log_record,int *offset) {
    std::lock_guard<std::mutex> lock(latch_);
    int record_length = log_record.RecordSize(); 
    int start_address = log_page_->GetInt(0); 
    int need_size = Page::MaxLength(record_length); 

    if(start_address + need_size > block_size_) {
        // sizeof(int) means the size of the page-header
        Flush();
        current_block_ = AppendNewBlock();
        start_address = log_page_->GetInt(0);
    }
    
    lastest_lsn_ ++; 
    log_record.SetLsn(lastest_lsn_); /* now, The lastestlsn corresponds to the current log */
    auto log_record_vector = log_record.Serializeto();
    log_page_->SetBytes(start_address, *log_record_vector);
    log_page_->SetInt(0, start_address + Page::MaxLength(record_length));
    *offset = current_block_.BlockNum() * block_size_ + start_address;
    return lastest_lsn_.load(); /* return the lsn of current log */
}

void LogManager::Flush() {
    file_manager_->Write(current_block_, *log_page_);
    last_flush_lsn_.store(lastest_lsn_); 
}

LogIterator LogManager::Iterator() {
    Flush(); /* because we will access the log file 
            which stored in disk, so should flush it */
    return LogIterator(file_manager_, BlockId(log_file_name_, 0));
}


LogIterator LogManager::Iterator(int block_number, int offset) {
    Flush();
    
    return LogIterator(file_manager_, 
            BlockId(log_file_name_, block_number), offset);
}

LogIterator LogManager::Iterator(int offset) {
    Flush();
    
    int block_number = offset/block_size_;
    int block_offset = offset%block_size_;
    
    return LogIterator(file_manager_, 
            BlockId(log_file_name_, block_number), block_offset);
}

} // namespace SimpleDB

#endif