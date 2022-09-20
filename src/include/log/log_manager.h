#ifndef LOGMANAGER_H
#define LOGMANAGER_H

#include <string>
#include <memory>
#include <mutex>

#include "file/file_manager.h"
#include "file/page.h"
#include "file/block_id.h"
#include "log/log_iterator.h"

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
    
    /**
    * @brief be always used when dirty pages are to be writen back to disk
    * 
    * @param lsn the lastest lsn of dirty page
    */
    void Flush(int lsn);

    /**
    * @brief append one log record to the file
    * Note that, this is an optimaization that first first write to the page
    * 
    * @param log_record char-array, logmanager does not know what the log-recode means
    * @return return lastest_lsn_ + 1 
    */
    int Append(const std::vector<char> &log_record);
    
    /**
    * @brief return the current log-iterator
    */
    LogIterator Iterator();
    
private:

    /**
    * @brief extend the log file one disk-block
    * 
    * @return the BlockId type of newly created disk-block
    */
    BlockId AppendNewBlock();
    
    /**
    * @brief flush the page to disk
    *   it will just be called by logmanager
    */
    void Flush();

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
    uint lastest_lsn_;
    // last lsn which saved to disk
    int last_saved_lsn_;
    // latch
    std::mutex latch_;
};

} // namespace SimpleDB
#endif