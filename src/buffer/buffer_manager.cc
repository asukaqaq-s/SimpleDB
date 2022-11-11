#ifndef BUFFER_MANAGER_CC
#define BUFFER_MANAGER_CC

#include "recovery/recovery_manager.h"
#include "buffer/buffer_manager.h"
#include "config/macro.h"

#include <iostream>

namespace SimpleDB {
    

// free_list_, replacer_, page_table_, available_num_, buffer_pool_
BufferManager::BufferManager(FileManager *fm, RecoveryManager *rm, int buffer_nums) 
    : file_manager_(fm), recovery_manager_(rm) {
    
    available_num_ = buffer_nums;
    for(int i = 0;i < buffer_nums;i ++) {
        auto buffer = std::make_unique<Buffer>(file_manager_);
        buffer_pool_.emplace_back(std::move(buffer));

        /* At the beginning, all buffer is unused */
        free_list_.push_back(i); 
    }
    
    replacer_ = std::make_unique<LRUReplacer>(buffer_nums);
}



frame_id_t BufferManager::VictimHelper() {
    frame_id_t frame_id;
    
    // 1. first select a unused buffer
    if(!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
        return frame_id;
    }

    // 2. select a victim buffer
    bool is_find = replacer_->Evict(&frame_id);

    // 3. if we can not find a available frame, return -1 means error
    if(!is_find && !available_num_) {
        return INVALID_FRAME_ID; /* fail */
    }
    
    return frame_id;
}

void BufferManager::PinHelper(frame_id_t frame_id, BlockId block) {
    auto buffer = buffer_pool_[frame_id].get();

    // if this block is not pinned, we should read this 
    // block's content into memory
    if(!buffer->IsPinned()) {
        available_num_--;
        replacer_->Pin(frame_id);
        page_table_[block] = frame_id;

        // read data
        buffer->AssignBlock(block, file_manager_, recovery_manager_);
    }
    buffer->pin();
}



void BufferManager::UnpinHelper(frame_id_t frame_id, BlockId block) {
    auto buffer = buffer_pool_[frame_id].get();

    buffer->unpin();

    // update replacer object, page table and buffer
    if(!buffer->IsPinned()) {
        available_num_ ++;
        replacer_->Unpin(frame_id);
        page_table_.erase(block);

        // if this buffer is dirty, we should flush informations to disk
        if (buffer->IsDirty()) {
            buffer->flush(file_manager_, recovery_manager_);
        }
        
        // notify other txns which acquire a free frame
        victim_cv_.notify_all();
    }
}



Buffer* BufferManager::TryToPin(BlockId block) {
    Buffer *buffer;
    frame_id_t frame_id;
    
    if (page_table_.find(block) != page_table_.end()) { 
        // if the corresponding block exist in pool, just use it
        frame_id = page_table_[block];

        SIMPLEDB_ASSERT(buffer_pool_[frame_id]->GetPinCount() > 0,
                        "pin count must > 0");
        PinHelper(frame_id, block);
    } else {
        // otherwise, find a victim buffer
        frame_id = VictimHelper();
        
        // in FetchPage method, return NUll make txn wait 
        // until a unpinned buffer occur
        if (frame_id == INVALID_FRAME_ID) {
            assert(available_num_ == 0);
            return nullptr;
        }
        
        // success
        SIMPLEDB_ASSERT(!buffer_pool_[frame_id]->IsDirty() && 
                         buffer_pool_[frame_id]->GetPinCount() == 0,
                         "a new buffer should not be dirty");

        PinHelper(frame_id, block);
    }

    
    buffer = buffer_pool_[frame_id].get();
    return buffer;
}

bool BufferManager::WaitTooLong(
    std::chrono::time_point<std::chrono::high_resolution_clock> start) {
    
    auto now = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    
    bool res = elapsed > max_time_;
    return res;
}

// if we can not find a victim buffer
// then should add this thread to wait list
Buffer* BufferManager::PinBlock(BlockId block) {
    std::unique_lock<std::mutex> lock(latch_);
    auto start = std::chrono::high_resolution_clock::now();
    Buffer *buffer = TryToPin(block);

    
    while (buffer == NULL && !WaitTooLong(start)) {
        
        // wait until a pinned buffer release
        victim_cv_.wait_for(lock, std::chrono::milliseconds(max_time_));
        // require the buffer again
        buffer = TryToPin(block);
    }

    if(!buffer) { // still can not acquire it
        throw std::runtime_error("wait too long while bufferpool pin");
    }
    
    return buffer;
}

bool BufferManager::UnpinBlock(Buffer *buffer) {
    std::unique_lock<std::mutex> lock(latch_);
    assert(buffer != nullptr);
    auto block = buffer->GetBlockID();
    
    // not exist
    if(page_table_.find(block) == page_table_.end()){
        return false;
    }

    auto frame_id = page_table_[block];
    UnpinHelper(frame_id, block);
    return true;
}

bool BufferManager::UnpinBlock(BlockId block) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // not exist
    if(page_table_.find(block) == page_table_.end()){
        return false;
    }

    auto frame_id = page_table_[block];
    UnpinHelper(frame_id, block);
    return true;
}


bool BufferManager::UnpinBlock(BlockId block, bool is_dirty) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // not exist
    if(page_table_.find(block) == page_table_.end()){
        return false;
    }
    
    auto frame_id = page_table_[block];
    auto *buffer = buffer_pool_[frame_id].get();
    buffer->is_dirty_ |= is_dirty;

    UnpinHelper(frame_id, block);
    return true;
}


void BufferManager::FlushAll() {
    std::lock_guard<std::mutex> lock(latch_);
    int buffer_pool_size = static_cast<int> (buffer_pool_.size());

    for(int i = 0;i < buffer_pool_size;i ++) {
        auto *buffer = buffer_pool_[i].get();
        buffer->flush(file_manager_, recovery_manager_);
    }
}

Buffer* BufferManager::NewBlock(const std::string &file_name, int *block_num) {
    std::unique_lock<std::mutex> lock(latch_);
    auto start = std::chrono::high_resolution_clock::now();
    auto block = file_manager_->Append(file_name);
    Buffer *buffer = TryToPin(block);

    while (buffer == NULL && !WaitTooLong(start)) {
        
        // wait until a pinned buffer release
        victim_cv_.wait_for(lock, std::chrono::milliseconds(max_time_));
        // require the buffer again
        buffer = TryToPin(block);
    }

    if(!buffer) { // still can not acquire it
        throw std::runtime_error("wait too long while bufferpool pin");
    }
    
    *block_num = block.BlockNum();
    return buffer;
}


// allocate a new page in disk-file
Buffer* BufferManager::NewPage(BlockId block) {
    file_manager_->Append(block.FileName());

    return TryToPin(block);
}

Buffer* BufferManager::NewPage(std::string file_name) {
    auto blk = file_manager_->Append(file_name);
    return TryToPin(blk);
}


} // namespace SimpleDB

#endif