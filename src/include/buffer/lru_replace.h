#ifndef LRU_REPLACE_H
#define LRU_REPLACE_H

#include "config/type.h"

#include <unordered_map>
#include <mutex>
#include <list>


namespace SimpleDB {


/**
* @brief the LRUReplacer can be used to bufferpool
In LRUReplacer, frames are organized into a queue, with the frame at the head of 
the squad being expelled first.Each time a newly used  each timeor newly entered frame 
enters the tail of the queueq
*/
class LRUReplacer{
public:
    /**
     * @brief Construct a new LRUReplacer object
     * 
     * @param num_pages the maximum number of pages/frames the LRU replacer
     * need to store. Currently we don't care this number since the implementation
     * of LRU algorithm don't have constrains on the number of slots.
     * However, we can exploit this by implementing our own hash table
     * and use num_pages length array as slot buffer to have better cache locality
     */
    explicit LRUReplacer(frame_id_t num_pages) {}

    /**
     * @brief Destroy the LRUReplacer object
     */
    ~LRUReplacer() = default;
    
    /**
    * @brief Remove the victim frame.
    * Each time the queue header frame pops up
    * 
    * @param victim the frame of being victimed
    * @return whether success
    */
    bool Evict(frame_id_t *victim);

    /**
    * @brief this specified frame will not enter the queue 
    * 
    * @param frame_id 
    */
    void Pin(frame_id_t frame_id);

    /**
    * @brief this specified frame will enter the queue
    * 
    * @param frame_id
    */
    void Unpin(frame_id_t frame_id);

    /**
    * @brief return the number of frame in the replacer 
    * that can be victim
    */
    int Size();
    
    // for debugging purpose
    void PrintLRU();

private:
    using list_t = std::list<frame_id_t>;
    
    // we view list as queue
    list_t list_;
    
    std::unordered_map<frame_id_t, list_t::iterator> table_;

    // if buffer pool manager is protected by lock, then we don't need this mutex
    // however, since linux will use futex as mutex, we just need a additional atomic instruction
    // which should be cheap. because contention occurs at high-level in the lock of buffer pool manager
    std::mutex latch_;

};

} // namespace SimpleDB

#endif