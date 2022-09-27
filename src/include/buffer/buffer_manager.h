#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include "log/log_manager.h"
#include "file/file_manager.h"
#include "buffer/buffer.h"
#include "buffer/lru_replace.h"
#include "file/block_id.h"

#include <list>
#include <map>
#include <chrono>

namespace SimpleDB {

/**
* @brief Manages the pinning and unpinning of buffers to blocks.
* 
*/
class BufferManager {
    
public:

    /**
    * @brief 
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * Since we use the buffer class to encapsulate reads and writes, 
    * it is not needed filemanager and logmanager stores in Buffermanager
    */
    explicit BufferManager(FileManager *fm, LogManager *lm, int buffer_nums);
    
    /**
    * @return the number of avaiable (unused, unpinned) buffers.
    */
    int available() {return available_num_;}

    /**
    * @brief pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed time period, 
    * will throw a exception
    * @param block a disk block
    * @return the buffer pinned to taht block 
    */
    Buffer* Pin(BlockId block);

    /**
    * @brief Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * 
    * @param buffer the buffer to be unpinned
    */
    bool Unpin(Buffer *buffer);

    /**
    * @brief another version Unpin
    */
    bool Unpin(BlockId block);

    /**
    * @brief
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
    void FlushAll(txn_id_t txn_num);
    
    /**
    * @brief
    * Flushes the all dirty buffers 
    */
    void FlushAll();

    
    /********* for debugging purpose *********/
    
    /**
    * @brief allocate a new page to disk-file
    * and move it to bufferpool
    * @param block 
    */
    Buffer* NewPage(BlockId block);

    Buffer* NewPage(std::string file_name);

private:
    /* some heapler functions */

    /**
    * @brief this function can help us find the
    *  victim frame quickly
    *
    * @return the position of a available buffer in pool 
    */
    frame_id_t VictimHelper();
    
    /**
    * @brief if a buffer is found, call this function help us
    * matain data memember
    *   this function assumes that we successfully find a victim buffer. 
    */
    void PinHelper(frame_id_t frame_id, BlockId block);

    /**
    * @brief 
    */
    void UnpinHelper(frame_id_t frame_id, BlockId block);
    
    /**
    * @brief Tries to pin a buffer to the specified block. 
    * 
    * @return a pointet which point to avaiable buffer 
    */
    Buffer* TryToPin(BlockId block);

    /*
    */
    bool WaitTooLong(
        std::chrono::time_point<std::chrono::high_resolution_clock>);
    
    // shared file_manager
    FileManager *file_manager_;
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
};  

} // namespace SimpleDB

#endif