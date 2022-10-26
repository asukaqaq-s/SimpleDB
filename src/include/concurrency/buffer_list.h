#ifndef BUFFER_LIST_H
#define BUFFER_LIST_H

#include "file/block_id.h"
#include "buffer/buffer_manager.h"
#include "config/macro.h"

#include <map>
#include <vector>
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
        SIMPLEDB_ASSERT(buffers_.find(block) != buffers_.end(), "file not exist");
        return buffers_.at(block);
    }

    /**
    * @brief Pin the block and keep track of the buffer internally.
    * @param block the disk block
    */
    void Pin(const BlockId &block) {

        Buffer *buffer = buffer_manager_->Pin(block);
        buffers_[block] = buffer;
        pins_.emplace_back(block);
        // note that: A block can store(pinned) 
        // several times and have several objects in list
    }

    /**
    * @brief Unpin the specified block.
    * @param block a reference to the disk block
    */
    void Unpin(const BlockId &block) {
        
        SIMPLEDB_ASSERT(buffers_.find(block) != buffers_.end(),
                        "unpin error");

        Buffer *buffer = buffers_.at(block);
        SIMPLEDB_ASSERT(buffer != NULL, "bufferlist error");
        buffer_manager_->Unpin(block);
        
        // find the block in pins_, because a block can store serveral times
        // we just unpin once and delete the block once
        // don't use list.remove(), it will delete all objects of this block
        auto iter = std::find(pins_.begin(), pins_.end(), block);
        if (iter != pins_.end()) {
            pins_.erase(iter);
        }
        
        // find the block twice, if we cannot find it again
        // mean that this block is not pinned
        iter = std::find(pins_.begin(), pins_.end(), block);
        if (iter == pins_.end()) {
            buffers_.erase(block);
        }
    }
    
    /**
    * @brief
    * Unpin any buffers still pinned by this transaction.
    * it usually be used when the transaction end.
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

    // we also can use a container: map buffer 
    // to {buffer *, int times} insteal of a map and a vector
    // every unpin ops, times --
    // every pin ops, times ++

    std::map<BlockId,Buffer*> buffers_;
    // serially stored in heap memory
    std::vector<BlockId> pins_;

    BufferManager *buffer_manager_;
};


} // namespace SimpleDB

#endif
