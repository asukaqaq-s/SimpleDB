#ifndef BUFFER_MANAGER_cc
#define BUFFER_MANAGER_cc

#include "buffer/buffer_manager.h"

#include <iostream>

namespace SimpleDB {

// free_list_, replacer_, page_table_, available_num_, buffer_pool_
BufferManager::BufferManager(FileManager *fm, LogManager *lm, int buffer_nums) {
    file_manager_ = fm;
    available_num_ = buffer_nums;
    for(int i = 0;i < buffer_nums;i ++) {
        auto buffer = std::make_unique<Buffer>(fm, lm);
        buffer_pool_.emplace_back(std::move(buffer));
        free_list_.push_back(i); /* At the beginning, all buffer is unused */
    }
    replacer_ = std::make_unique<LRUReplacer>(buffer_nums);
}

// 1. first select unused buffer
// 2. select a victim buffer
// 3. if we can not find a available frame, return -1 means error
frame_id_t BufferManager::VictimHelper() {
    frame_id_t frame_id;
    if(!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
        return frame_id;
    }
    // 2. select a victim buffer
    bool is_find = replacer_->Evict(&frame_id);
    if(!is_find && !available_num_) {
        return -1; /* fail */
    }
    return frame_id;
}

void BufferManager::PinHelper(frame_id_t frame_id, BlockId block) {
    auto buffer = buffer_pool_[frame_id].get();

    if(!buffer->IsPinned()) {
        available_num_--;
        replacer_->Pin(frame_id);
        // block
        page_table_[block] = frame_id;
        buffer->AssignBlock(block);
    }
    buffer->pin();
}

void BufferManager::UnpinHelper(frame_id_t frame_id, BlockId block) {
    auto buffer = buffer_pool_[frame_id].get();

    buffer->unpin();
    if(!buffer->IsPinned()) {
        available_num_ ++;
        replacer_->Unpin(frame_id);
        // block
        page_table_.erase(block);
        if(buffer->ModifyingTrx() != -1) 
            buffer->flush();
        victim_cv_.notify_all();
    }
}

// 1. if the corresponding block exist in pool, just use it
// 2. otherwise, find a victim buffer
// 3. if can not find one, wait to a buffer unpinned
Buffer* BufferManager::TryToPin(BlockId block) {
    Buffer *buffer;
    frame_id_t frame_id;

    if(page_table_.find(block) != page_table_.end()) { /* can find it */
        // step 1
        frame_id = page_table_[block];
        PinHelper(frame_id, block);
    } else { /* can not find */
        // step 2
        frame_id = VictimHelper();
        if(frame_id == -1) {
            assert(available_num_ == 0);
            return NULL; /* will wait until a unpinned buffer occur*/
        }
        assert(buffer_pool_[frame_id]->GetPinCount() == 0);
        
        PinHelper(frame_id, block);
    }

    assert(!buffer_pool_[frame_id]->IsDirty()); /* should not dirty */
    
    buffer = buffer_pool_[frame_id].get();
    return buffer;
}

bool BufferManager::WaitTooLong(
std::chrono::time_point<std::chrono::high_resolution_clock> start) {
    auto now = std::chrono::high_resolution_clock::now();
    double elapsed = 
        std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    
    bool res = elapsed > max_time_;
    return res;
}

// if we can not find a victim buffer
// then should add this thread to wait list
Buffer* BufferManager::Pin(BlockId block) {
    std::unique_lock<std::mutex> lock(latch_);
    auto start = std::chrono::high_resolution_clock::now();
    Buffer *buffer = TryToPin(block);

    while (buffer == NULL && !WaitTooLong(start)) {
        victim_cv_.wait_for(lock, std::chrono::milliseconds(max_time_));
        buffer = TryToPin(block); /* require the buffer again */
    }

    if(!buffer) { // still can not acquire it
        throw std::runtime_error("wait too long while bufferpool pin");
    }
    
    return buffer;
}

bool BufferManager::Unpin(Buffer *buffer) {
    assert(buffer != nullptr);
    std::unique_lock<std::mutex> lock(latch_);
    auto block = buffer->BlockNum();
    
    if(page_table_.find(block) == page_table_.end()){
        return false;
    }
    auto frame_id = page_table_[block];
    UnpinHelper(frame_id, block);
    
    return true;
}

bool BufferManager::Unpin(BlockId block) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if(page_table_.find(block) == page_table_.end()){
        return false;
    }
    auto frame_id = page_table_[block];
    UnpinHelper(frame_id, block);
    return true;
}

void BufferManager::FlushAll(txn_id_t txn_num) {
    std::lock_guard<std::mutex> lock(latch_);
    int buffer_pool_size = static_cast<int> (buffer_pool_.size());

    for(int i = 0;i < buffer_pool_size;i ++) {
        auto *buffer = buffer_pool_[i].get();
        if(buffer->ModifyingTrx() == txn_num) {
            buffer->flush();
        }
    }
}

void BufferManager::FlushAll() {
    std::lock_guard<std::mutex> lock(latch_);
    int buffer_pool_size = static_cast<int> (buffer_pool_.size());

    for(int i = 0;i < buffer_pool_size;i ++) {
        auto *buffer = buffer_pool_[i].get();
        buffer->flush();
    }
}

// 1. allocate a new page in disk-file
// 2. 
Buffer* BufferManager::NewPage(BlockId block) {
    file_manager_->Append(block.FileName());

    return Pin(block);
}

Buffer* BufferManager::NewPage(std::string file_name) {
    auto blk = file_manager_->Append(file_name);
    return Pin(blk);
}

} // namespace SimpleDB

#endif