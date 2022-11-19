#ifndef BUFFER_H
#define BUFFER_H

#include "file/file_manager.h"
#include "config/rw_latch.h"
#include "config/type.h"

#include <cstring>

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


    explicit Buffer(FileManager *file_manager) {
        data_ = std::make_unique<Page> (file_manager->BlockSize());         
    }


    /**
    * @brief return a data ptr which wrapped by page object
    */    
    inline Page* contents() { 
        return data_.get(); 
    }


    /**
    * @brief Mark as dirty pages and set the lastest lsn of buffer
    */
    inline void SetPageLsn(lsn_t lsn) {
        if (lsn > INVALID_LSN) {
            SIMPLEDB_ASSERT(lsn >= data_->GetLsn(), "lsn error");
            data_->SetLsn(lsn);
            is_dirty_ = true;
        }
    }


    /**
    * @brief Get the PageLsn
    */
    inline lsn_t GetPageLsn() const { 
        return data_->GetLsn(); 
    }


    /**
    * @brief init page type
    */
    inline void SetPageType(PageType type) {
        data_->SetPageType(type);
    }


    /**
    * @brief get page type
    */
    inline PageType GetPageType() const {
        return data_->GetPageType();
    }


    /**
    * @return check if the buffer is pinned
    */
    inline bool IsPinned() const { 
        return pin_count_ > 0; 
    } 


    /**
    * @brief always called by newblock method
    */
    void Init() {
        int size = data_->GetSize();
        memset(data_->GetRawDataPtr(), 0, size);
    }

    
    inline void pin() { 
        pin_count_++; 
    }
    
    inline void unpin() { 
        pin_count_--; 
    }

    inline int GetPinCount() { 
        return pin_count_; 
    }

    inline const BlockId &GetBlockID() { 
        return block_; 
    }
    
    inline bool IsDirty() { 
        return is_dirty_; 
    }

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