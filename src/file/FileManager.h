#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include <fstream>
#include <map>
#include <mutex>

#include "Page.h"
#include "Blockid.h"
#include "Page.cc"
#include "Blockid.cc"

namespace SimpleDB {

/**
* @brief we use file-level to access disk and view a file as a raw disk
*   through page-level to access the file  
* FileManager implements write/read pages to/from disk-blocks
*/
class FileManager {
    
public:
    /**
    * @brief create a filemanager, only one object exist in Simpledb::client
    *   
    * @param db_directory direcotry path name (linux)
    * @param block_size
    */
    FileManager(const std::string &db_directory, int block_size);

    /**
    * @brief read a page from disk-block
    * 
    * @param block logical block number
    * @param page page
    */
    void Read(const BlockId &block, Page &page);

    /**
    * @brief write a page's content to disk-block
    * every write operation must be written immediately to the disk. 
    * 
    * @param block logical block number
    * @param page
    */
    void Write(const BlockId &block, Page &page);
    
    /**
    * @brief seek to the end of the file and writes an empty array of bytes to it,
    *  causes the OS to automatically extend the file
    * 
    * @param file_name
    */
    BlockId Append(const std::string &file_name);
    
    /**
    * @brief
    */
    bool IsNew() { return is_new_; }
    
    /**
    * @brief
    */
    int BlockSize() { return block_size_; }
    
    /**
    * @brief return the next logical block number of the file
    * i.e.The file has only one block: block 0, so the size of file is 4kb 
    *   but if we call Length func, will return logical number 1
    *   Actually block 1 doesn't belong to us, OS will automatically expand the size of file
    *   we can expand the file in this-way
    * p.s The length of the file is always a multiple of 4kb
    */
    int Length(const std::string &file_name) {
        return GetFileSize(file_name) / block_size_;
    }

private:

    /**
    * @brief Get the corresponding file-stream by file name
    * 
    * @param file_name
    */
    std::shared_ptr<std::fstream> GetFile(const std::string &file_name);
    
    /**
    * @brief Get the file'size by file name
    * 
    * @param file_name
    */
    int GetFileSize(const std::string &file_name);

    // directory name
    std::string directory_name_;
    // disk block fix-size
    int block_size_;
    // is this db newly created?
    bool is_new_;
    // map file_name to pointer which point to their fstream and cache it(optimization)
    std::map<std::string, std::shared_ptr<std::fstream>> open_files_;
    // mutex latch
    std::mutex latch_;
    // may use  int next_page_id;
    
    // for analyze
    std::chrono::milliseconds data_write_time_{0};
    std::chrono::milliseconds data_read_time_{0};
};

} // namespace SimpleDB

#endif