#ifndef LOGMANAGER_H
#define LOGMANAGER_H

#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

#include "file/file_manager.h"
#include "file/page.h"
#include "file/block_id.h"
#include "log/log_iterator.h"
#include "config/type.h"

namespace SimpleDB {

/**
* @brief 
    The DB has a log file and only one LogManager Object exist in system.
The LogManager object is used to write and read a log-record in log file,
and the object is called by the recovery system to restore data which stored 
in disk through the log.
    Note that, LogManager does not know the meaning of the stored data and 
how to produce a log-record, it is only responsible for storage  and  not 
responsible for recovery data.
*/

class LogManager {

public:

    /**
    * @brief Logmanager and Simpledb share a filemanager object
    * 
    * @param file_manager Accepts a pointer parameter
    * @param log_file_name    
    */
    LogManager(FileManager* file_manager, std::string log_file_name);
    
    ~LogManager() = default;
    /**
    * @brief be always used when dirty pages are to be writen back to disk
    * 
    * @param lsn the lastest lsn of dirty page
    */
    void Flush(lsn_t lsn);

    /**
    * @brief append one log record to the file
    * Note that, this is an optimaization that first write to the page
    * 
    * @param log_record char-array, logmanager does not know what the log-recode means
    * @return return lastest_lsn_ + 1 
    */
    lsn_t Append(const std::vector<char> &log_record);
    
    lsn_t AppendLogRecord(std::vector<char> log_record);

    /**
    * @brief return the current log-iterator
    */
    LogIterator Iterator();

    void SetLastestLsn(lsn_t lsn) { lastest_lsn_ = lsn; }

private:

    /**
    * @brief extend one disk-block size of the log file
    * 
    * @return the BlockId type of newly created disk-block
    */
    BlockId AppendNewBlock();
    
    /**
    * @brief flush the page to disk
    *   it will just be called by logmanager
    */
    void Flush();
    

    /* data memember */
    
    // only one filemanager object exist in system, so it is a pointer type
    // we use the file_manager_ to write logs to disk 
    FileManager * file_manager_;
    // file_name, is not a path
    std::string log_file_name_;
    // a page in memeory 
    std::unique_ptr<Page> log_page_;
    // lastest written block
    BlockId current_block_;
    // lastest lsn 
    std::atomic<lsn_t> lastest_lsn_;
    // last lsn which saved to disk
    std::atomic<lsn_t> last_saved_lsn_;
    // latch
    std::mutex latch_;
    
    /********* advance log-manager *********/
    // TODO ....
    // a flush buff in memory
    // std::unique_ptr<Page> flush_page_;
    // background thread for flushing 
    // std::unique_ptr<std::thread> flush_thread_;
    // whether flush_thread working?
    // std::atomic<bool> enable_flushing_;
    // cv used to wakeup the background thread
    // std::condition_variable flush_cv_;
    // cv used to block normal operation
    // std::condition_variable operation_cv_;
    // sleep time
    // std::chrono::milliseconds sleep_time_{300};
    /****************************************/
};

} // namespace SimpleDB
#endif