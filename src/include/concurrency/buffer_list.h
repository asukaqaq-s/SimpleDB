#ifndef BUFFER_LIST_H
#define BUFFER_LIST_H

#include "file/block_id.h"
#include "buffer/buffer_manager.h"
#include "config/macro.h"

#include <map>
#include <list>
#include <algorithm>
#include <iostream>

namespace SimpleDB {

class BufferList {

public:

    BufferList(BufferManager *bm) : buffer_manager_(bm) {}

    /**
    * @brief return the buffer pinned to the specified block.
    * The method returns null if the transaction has not
    * pinned the block.
    * @param block the disk block
    * @return the buffer pinned to that block
    */
    Buffer* GetBuffer(const BlockId &block) {
        return buffers_.at(block);
    }

    /**
    * @brief Pin the block and keep track of the buffer internally.
    * @param block the disk block
    */
    void Pin(const BlockId &block) {
        Buffer *buffer = buffer_manager_->Pin(block);
        buffers_[block] = buffer;
        pins_.push_back(block);
    }

    /**
    * @brief Unpin the specified block.
    * @param block a reference to the disk block
    */
    void Unpin(const BlockId &block) {
        Buffer *buffer = buffers_.at(block);
        SIMPLEDB_ASSERT(buffer != NULL, "bufferlist error");
        buffer_manager_->Unpin(block);
        pins_.remove(block);
        // find pin_
        auto iter = std::find(pins_.begin(), pins_.end(), block);
        if(iter == pins_.end()) {
            buffers_.erase(block);
        }
    }
    
    /**
    * @brief
    * Unpin any buffers still pinned by this transaction.
    */
    void UnpinAll() {
        for (auto block : pins_) {
            buffers_.erase(block);
            buffer_manager_->Unpin(block);
        }
        buffers_.clear();
        pins_.clear();
    }
    

private:

    std::map<BlockId,Buffer*> buffers_;
    
    std::list<BlockId> pins_;

    BufferManager *buffer_manager_;

};


} // namespace SimpleDB

#endif
