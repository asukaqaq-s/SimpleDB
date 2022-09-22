#ifndef LRU_REPLACE_CC
#define LRU_REPLACE_CC

#include "buffer/lru_replace.h"

#include <iostream>

namespace SimpleDB {
    
bool LRUReplacer::Evict(frame_id_t *victim) {
    std::lock_guard<std::mutex> lock(latch_);
    int queue_size = list_.size();

    if(queue_size == 0) { /* empty */
        return false;
    }
    /* success */
    *victim = list_.front();
    list_.pop_front(); /* pop up */
    table_.erase(*victim);
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    if(table_.find(frame_id) != table_.end()) {
        auto it = table_[frame_id];
        table_.erase(frame_id);
        list_.erase(it);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    if(table_.find(frame_id) == table_.end()) { /* not found */
        list_.push_back(frame_id);
        table_[frame_id] = prev(list_.end());
    }
}

int LRUReplacer::Size() {
    return list_.size();
}


void LRUReplacer::PrintLRU() {
    std::cout << "Print start" << std::endl;
    int cnt = 0;
    for(auto t:list_) {
        std::cout << "number " << cnt++ << ": "
        << t << std::endl;
    }
    std::cout << "Print end" << std::endl;
}

} // namespace SimpleDB

#endif