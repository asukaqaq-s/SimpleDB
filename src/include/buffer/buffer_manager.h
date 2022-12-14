#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <list>
#include <map>
#include <chrono>

#include "file/file_manager.h"
#include "log/log_manager.h"
#include "buffer/lru_replace.h"
#include "buffer/buffer.h"
#include "config/rw_latch.h"

namespace SimpleDB {


class RecoveryManager;

/**
 * @brief BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferManager {


public:

    /**
    * @brief 
    * Creates a buffer manager having the specified numbers of buffer slots.
    * Since we use the buffer class to encapsulate reads and writes.
    */
    explicit BufferManager(FileManager *fm, RecoveryManager *rm, int buffer_nums);
    

    /**
    * @return the number of avaiable (unused, unpinned) buffers.
    */
    int available() {return available_num_;}
    
    
    /**
    * @brief pins a buffer to the specified block, potentially waiting until a buffer 
    * becomes available.If no buffer becomes available within a fixed time period, 
    * will throw a exception
    * @param block a disk block
    * @return the buffer pinned to taht block 
    */
    Buffer* PinBlock(const BlockId &block);


    /**
    * @brief Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * 
    * @param buffer the buffer to be unpinned
    */
    bool UnpinBlock(Buffer *buffer);


    /**
    * @brief unpin a block
    */
    bool UnpinBlock(const BlockId &block, bool is_dirty = false);
    

    /**
    * @brief Flush all dirty buffers which exists in bufferpool
    */
    void FlushAll();
    

    /**
    * @brief acquire a new block which not create
    */
    Buffer* NewBlock(const std::string &file_name, int *block_num);


    Buffer* NewBlock(const std::string &file_name);
    
    
    /********* for debugging purpose *********/
    

    void GetBufferPoolConsumeTime();


    void PrintBufferPool() {
        std::cout << "buffer pool = " << buffer_pool_.size() << std::endl;
        for (auto &t:buffer_pool_) {
            if (t->IsPinned())
                std::cout << t->GetPinCount() << " " << t->GetBlockID().to_string() << std::endl;
        }

        std::cout << std::endl;
    }


    bool CheckPinCount() {
        for (auto &t:buffer_pool_) {
            if (t->GetPinCount() > 0) {
                return false;
            }
        }

        return true;
    }


private: // some heapler functions


    /**
    * @brief 
    * this function can help us find the victim frame quickly
    * @return the position of a available buffer in pool 
    */
    frame_id_t VictimHelper();
    

    /**
    * @brief if a buffer is found, call this function help us matain data memember
    * this function assumes that we successfully find a victim buffer. 
    */
    void PinHelper(frame_id_t frame_id, const BlockId &block, bool write_to_disk = true);

    /**
    * @brief 
    */
    void UnpinHelper(frame_id_t frame_id, const BlockId &block);


    void FlushHelper(Buffer *buffer);
    
    /**
    * @brief 
    * Tries to pin a buffer to the specified block. 
    * @return a pointet which point to avaiable buffer 
    */
    Buffer* TryToPin(const BlockId &block);


    Buffer* TryToAllocatePin(const BlockId &block);


    bool WaitTooLong(std::chrono::time_point<std::chrono::high_resolution_clock>);






private:


    // shared file_manager
    FileManager *file_manager_;

    // shared recovery_manager
    RecoveryManager *recovery_manager_;

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

    double pin_time_{0};
    double unpin_time_{0};


};  

} // namespace SimpleDB

#endif