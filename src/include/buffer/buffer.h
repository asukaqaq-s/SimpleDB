#ifndef BUFFER_H
#define BUFFER_H

#include "config/type.h"
#include "file/file_manager.h"
#include "log/log_manager.h"


#include <memory>
#include <assert.h>

namespace SimpleDB {

/**
* @brief a buffer is a unit of the bufferpool
To better manage the buffer pool, we use buffer class to replacer
page class.
*/
class Buffer {
    
public:

    explicit Buffer(FileManager *fm, LogManager *lm)
    : file_manager_(fm), log_manager_(lm) {
        contents_ = 
            std::make_unique<Page> (file_manager_->BlockSize());           
    }
    
    /**
    * @brief 
    * 
    * @return content be used to modify
    */
    Page* contents() { return contents_.get(); }

    /**
    * @brief 
    */
    void SetModified(txn_id_t trx_num, int lsn) {
        trx_num_ = trx_num;
        if(lsn >= 0) {
            lsn_ = lsn;
        }
    }

    bool IsPinned() { return pin_ > 0; } 

    int ModifyingTrx() { return trx_num_; }

    /**
    * @brief assign a new block, 
    * and change its modifying trx 
    */
    void AssignBlock(BlockId blk) {
        flush(); /* through calling flush func in AssignBlock, in 
                    bufferpool pin a page, we don't need to care 
                    about wirting dirty page to disk*/
        block_ = blk;
        file_manager_->Read(block_, *contents_);
        pin_ = 0; 
    }

    /**
    * @brief usually be used to commit a transaction
    */
    void flush() {
        if (trx_num_ >= 0) {
            log_manager_->Flush(lsn_);
            file_manager_->Write(block_, *contents_);
            trx_num_ = -1;
        }
    }

    void pin() { pin_++; }
    
    void unpin() { pin_--; }

    BlockId BlockNum() { return block_;}

    BlockId GetBlockID() { return block_; }
    
    int GetPinCount() { return pin_; }
    
    // for debugging purpose
    // because of AssignBlock, we don't need to this function
    bool IsDirty() { return trx_num_ != -1; }

private:
    
    // shared filemanager
    FileManager* file_manager_;
    // shared logmanager
    LogManager* log_manager_;
    // buffer content
    std::unique_ptr<Page> contents_;
     
    BlockId block_;
    
    int pin_ = 0;
    // if trx_num != -1, means the page is a dirty_page
    txn_id_t trx_num_ = -1;
    // the LSN of the most recent log record.
    lsn_t lsn_ = -1;
};

} // namespace SimpleDB

#endif 