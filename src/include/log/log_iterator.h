#ifndef LOGITERATOR_H
#define LOGITERATOR_H

#include "file/block_id.h"
#include "file/page.h"
#include "file/file_manager.h"

#include <memory>

namespace SimpleDB {

/**
* @brief a logiterator object, can be used to access the log file
    which stored in disk.
*/
class LogIterator {
    
public:
    /**
    * @brief constructor
    *   call MoveToBlock func to access the first block of the log file 
    */
    LogIterator(FileManager *file_manager, BlockId block)
    : file_manager_(file_manager) {
        block_ = block;
        page_ = std::make_shared<Page> (file_manager_->BlockSize());
        MoveToBlock(block_);
        log_file_size_ = file_manager_->Length(block_.FileName()) - 1;
    }
    
    /**
    * @brief constructor
    *   call MoveToBlock func to access the specified block of the log file 
    * 
    * @param file_manager 
    * @param block 
    * @param offset the offset in the specified block
    */
    LogIterator(FileManager *file_manager, BlockId block, int offset)
    : file_manager_(file_manager) {
        block_ = block;
        page_ = std::make_shared<Page> (file_manager_->BlockSize());
        MoveToBlock(block_);
        log_file_size_ = file_manager_->Length(block_.FileName());
        current_pos_ = offset;
    }

    /**
    * @brief 
    */
    bool HasNextRecord();
    
    /**
    * @brief move to next log record
    */
    std::vector<char> NextRecord();
    
    /**
    * @brief move to the specified position which
    * must less than current postion
    * 
    * @param offset the offset of the log file
    */
    std::vector<char> MoveToRecord(int offset);


    int GetLogOffset() {
        if(current_pos_ == boundary_) {
            return (block_.BlockNum() + 1) * file_manager_->BlockSize() + sizeof(int);
        } else {
            return block_.BlockNum() * file_manager_->BlockSize() + current_pos_;
        }
    }
    
private:
    /**
    * @brief the logiterator move to the Specified block
    *   is always used to Contructor or read finish a block.
    * 
    * @param block the specified block
    */
    void MoveToBlock(BlockId block);

    // shared file_manager
    FileManager* file_manager_;
    
    BlockId block_;
    // read buff
    std::shared_ptr<Page> page_;
    // the position in block
    int current_pos_;
    // the boundary in block, be used to ensure whether the access ends
    int boundary_;
    // the size of log file
    int log_file_size_;
};

} // namespace SimpleDB

#endif