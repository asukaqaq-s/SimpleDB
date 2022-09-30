#ifndef LOGITERATOR_H
#define LOGITERATOR_H

#include "file/block_id.h"
#include "file/page.h"
#include "file/file_manager.h"

#include <memory>

namespace SimpleDB {

/**
* @brief a logiterator object, can be used to access the log file
    which stored in disk.
*/
class LogIterator {
    
public:
    
    /**
    * @brief 
    */
    LogIterator(FileManager *file_manager, std::string file_name, int offset);

    /**
    * @brief whether has next record
    * if current_pos == log_file_size, return true
    * else return false
    */
    bool HasNextRecord();
    
    /**
    * @brief move to next log record
    * cache to reduce io cost
    */
    void NextRecord();
    
    /**
    * @brief return the current log record
    * 
    * @return the byte array of log record  
    */
    std::vector<char> CurrentRecord();

    /**
    * @brief move to the specified position 
    * 
    * @param offset the offset of the log file
    * @return the byte-array of the specified log record
    */
    std::vector<char> MoveToRecord(int offset);

    int GetLogOffset() {
        return file_offset_;
    }
    
private:

    // shared file_manager
    FileManager* file_manager_;
    // log file name
    std::string log_name_;
    // read buff
    std::unique_ptr<Page> read_buf_;
    // read buffer size
    int buffer_size_;
    // read count
    int buffer_offset_;
    // the position in log file
    int file_offset_;
    // the size of log file 
    int log_file_size_;
};

} // namespace SimpleDB

#endif