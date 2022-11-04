#ifndef BUFFER_H
#define BUFFER_H

#include "file/file_manager.h"
#include "config/rw_latch.h"
#include "config/type.h"

namespace SimpleDB {


/**
* @brief a buffer object is stored in memory and is a unit of bufferpool
* buffer also contains book-keeping information that is used by the buffer 
* pool manager, e.g.pin count, dirty flag, block id, etc.
*
* note that, page is only a wrapper for data, but not a unit of bufferpool
*/
class Buffer {

public:

    explicit Buffer(FileManager *file_manager);
    
    Page* contents() { return data_.get(); }

    /**
    * @brief Mark as dirty pages and set the lastest lsn of buffer
    */
    void SetPageLsn(lsn_t lsn);

    /**
    * @brief Get the PageLsn
    */
    inline lsn_t GetPageLsn() const;

    /**
    * @return check if the buffer is pinned
    */
    bool IsPinned() { return pin_count_ > 0; } 

    
    /**
    * @brief assign a new block and reset information
    */
    void AssignBlock(BlockId blk, 
                     FileManager *file_manager, 
                     RecoveryManager *recovery_manager);


    /**
    * @brief according to WAL, the log is flushed before the buffer 
    * is written back to disk 
    */
    void flush(FileManager *file_manager, RecoveryManager *recovery_manager);


    inline void pin() { pin_count_++; }
    
    inline void unpin() { pin_count_--; }

    inline BlockId BlockNum() { return block_;}

    inline BlockId GetBlockID() { return block_; }
    
    inline int GetPinCount() { return pin_count_; }
    
    // for debugging purpose
    // because of AssignBlock method, we don't need to this function maybe.
    inline bool IsDirty() { return is_dirty_; }

    // avoid multiple transaction access at the same time
    inline void RLock() { latch.RLock(); }
    
    inline void RUnlock() { latch.RUnlock(); }

    inline void WLock() { latch.WLock(); }

    inline void WUnlock() { latch.WUnlock(); }



protected:
    
    // buffer content
    std::unique_ptr<Page> data_;
 
    BlockId block_;

    int pin_count_ = 0;

    bool is_dirty_{false};
    
    // read write latch
    // because we need different transaction isolation level
    // Despite having a buffer lock, it is still needed different 
    // transaction work at the same time.so we should need to a
    // reader writer latch
    ReaderWriterLatch latch;

};

}


#endif