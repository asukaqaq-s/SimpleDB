#ifndef LOG_MANAGER_CC
#define LOG_MANAGER_CC

#include "log/log_manager.h"

#include <vector>

namespace SimpleDB {

// if the log_file is empty, assign a new block to it
// otherwise, open the last block
LogManager::LogManager(FileManager* file_manager, std::string log_file_name) :
    file_manager_(file_manager), log_file_name_(log_file_name) {
    int log_size = file_manager_->Length(log_file_name_); /* the next logical block number */
    std::shared_ptr<std::vector<char>> 
        array = std::make_shared<std::vector<char>>(file_manager->BlockSize(), 0);

    log_page_ = std::make_unique<Page>(array);
    
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
    log_page_->SetInt(0, file_manager_->BlockSize());
    file_manager_->Write(block, *log_page_);
    return block;
}

void LogManager::Flush(int lsn) {
    if(lsn >= last_saved_lsn_ ) { /* the record corresponding to 
                                last_saved_lsn_ is not generated */
        Flush();
    }
}

// note that the log records in the page from right to left
// and the size of a log record which stores in disk is equal
// to (log_record_length + sizeof(int))
int LogManager::Append(const std::vector<char> &log_record) {
    std::lock_guard<std::mutex> lock(latch_);
    int record_length = log_record.size(); 
    int page_boundary = log_page_->GetInt(0);
    int need_size = record_length + sizeof(int);
    int record_pos;
    
    if(page_boundary < static_cast<int>(sizeof(int)) + need_size) {
        // sizeof(int) means the size of the page-header
        Flush();
        current_block_ = AppendNewBlock();
        page_boundary = log_page_->GetInt(0);
    }
    record_pos = page_boundary - need_size;
    log_page_->SetBytes(record_pos, log_record);
    log_page_->SetInt(0, record_pos);
    lastest_lsn_ ++; 
    return lastest_lsn_; // the log of lastest_lsn_ is not generated  
}

void LogManager::Flush() {
    file_manager_->Write(current_block_, *log_page_);
    last_saved_lsn_ = lastest_lsn_; 
}

LogIterator LogManager::Iterator() {
    Flush(); /* because we will access the log file 
            which stored in disk, so should flush it */
    return LogIterator(file_manager_, current_block_);
}

} // namespace SimpleDB

#endif