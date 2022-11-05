#ifndef BUFFER_H
#define BUFFER_H

#include "file/file_manager.h"
#include "config/rw_latch.h"
#include "config/type.h"

namespace SimpleDB {


class RecoveryManager;

/**
* @brief a buffer object is stored in memory and is a unit of bufferpool
* buffer also contains book-keeping information that is used by the buffer 
* pool manager, e.g.pin count, dirty flag, block id, etc.
*
* note that, page is only a wrapper for data, but not a unit of bufferpool.
*/
class Buffer {

friend class BufferManager;

public:


    explicit Buffer(FileManager *file_manager);

    /**
    * @brief return a data ptr which wrapped by page object
    */    
    Page* contents() { return data_.get(); }


    /**
    * @brief Mark as dirty pages and set the lastest lsn of buffer
    */
    void SetPageLsn(lsn_t lsn);


    /**
    * @brief Get the PageLsn
    */
    inline lsn_t GetPageLsn() { return data_->GetLsn(); }


    /**
    * @return check if the buffer is pinned
    */
    bool IsPinned() { return pin_count_ > 0; } 

    
    /**
    * @brief assign a new block and reset information
    * 
    * @param file_manager shared filemangaer ptr which offered by bufferpool
    * @param recovery_manager shared recoverymanager ptr which offered by bufferpool
    */
    void AssignBlock(BlockId blk, 
                     FileManager *file_manager, 
                     RecoveryManager *recovery_manager);


    /**
    * @brief flush data to disk.
    * 
    * @param file_manager shared filemangaer ptr which offered by bufferpool
    * @param recovery_manager shared recoverymanager ptr which offered by bufferpool
    */
    void flush(FileManager *file_manager, RecoveryManager *recovery_manager);


    inline void pin() { pin_count_++; }
    
    inline void unpin() { pin_count_--; }

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
    // Despite having a block lock, it is still needed different 
    // transaction work at the same time.so we should need to a
    // reader writer latch
    ReaderWriterLatch latch;

};

}


#endif