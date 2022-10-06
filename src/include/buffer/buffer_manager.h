#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <list>
#include <map>
#include <chrono>

#include "file/file_manager.h"
#include "log/log_manager.h"
#include "buffer/lru_replace.h"
#include "config/rw_latch.h"

namespace SimpleDB {


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

    
    // avoid multiple transaction access at the same time
    void RLock() { latch.RLock(); }
    
    void RUnlock() { latch.RUnlock(); }

    void WLock() { latch.WLock(); }

    void WUnlock() { latch.WUnlock(); }

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
    txn_id_t trx_num_{INVALID_TXN_ID};
    // the LSN of the most recent log record.
    lsn_t lsn_{INVALID_LSN};
    // because we need different transaction isolation level
    // Despite having a buffer lock, it is still needed different 
    // transaction work at the same time.so we should need to a
    // reader writer latch
    ReaderWriterLatch latch;
};




/**
* @brief Manages the pinning and unpinning of buffers to blocks.
* 
*/
class BufferManager {
    
public:

    /**
    * @brief 
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * Since we use the buffer class to encapsulate reads and writes, 
    * it is not needed filemanager and logmanager stores in Buffermanager
    */
    explicit BufferManager(FileManager *fm, LogManager *lm, int buffer_nums);
    
    /**
    * @return the number of avaiable (unused, unpinned) buffers.
    */
    int available() {return available_num_;}
    
    /**
    * @brief pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed time period, 
    * will throw a exception
    * @param block a disk block
    * @return the buffer pinned to taht block 
    */
    Buffer* Pin(BlockId block);

    /**
    * @brief Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * 
    * @param buffer the buffer to be unpinned
    */
    bool Unpin(Buffer *buffer);

    /**
    * @brief another version Unpin
    */
    bool Unpin(BlockId block);

    /**
    * @brief
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
    void FlushAll(txn_id_t txn_num);
    
    /**
    * @brief
    * Flushes the all dirty buffers 
    */
    void FlushAll();
    
    /********* for debugging purpose *********/
    
    /**
    * @brief allocate a new page to disk-file
    * and move it to bufferpool
    * @param block 
    */
    Buffer* NewPage(BlockId block);

    Buffer* NewPage(std::string file_name);

private:
    /* some heapler functions */

    /**
    * @brief this function can help us find the
    *  victim frame quickly
    *
    * @return the position of a available buffer in pool 
    */
    frame_id_t VictimHelper();
    
    /**
    * @brief if a buffer is found, call this function help us
    * matain data memember
    *   this function assumes that we successfully find a victim buffer. 
    */
    void PinHelper(frame_id_t frame_id, BlockId block);

    /**
    * @brief 
    */
    void UnpinHelper(frame_id_t frame_id, BlockId block);
    
    /**
    * @brief Tries to pin a buffer to the specified block. 
    * 
    * @return a pointet which point to avaiable buffer 
    */
    Buffer* TryToPin(BlockId block);

    /*
    */
    bool WaitTooLong(
        std::chrono::time_point<std::chrono::high_resolution_clock>);
    
    // shared file_manager
    FileManager *file_manager_;
    // starring role
    std::vector<std::unique_ptr<Buffer>> buffer_pool_;
    // unused buffer
    std::list<frame_id_t> free_list_;
    // unpined buffer or unused buffer
    int available_num_;
    // map blockid to frame_id, differ from page_table in os
    std::map<BlockId, frame_id_t> page_table_;
    // replacer algorithm
    std::unique_ptr<LRUReplacer> replacer_;
    // big latch, currently it will protect whole buffer pool manager
    std::mutex latch_;
    // be used to select a victim
    std::condition_variable victim_cv_;
    
    /* wait time can be setted by user */
    std::chrono::milliseconds milliseconds{10000};
    const int max_time_ = 10000;
};  

} // namespace SimpleDB

#endif