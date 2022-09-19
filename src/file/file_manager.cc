#ifndef FILE_MANAGER_CC
#define FILE_MANAGER_CC

#include "file/file_manager.h"

#include <iostream>
#include <sys/stat.h>
#include <filesystem>
#include <unistd.h>
#include <cstring>

namespace SimpleDB {

// if the directory is not exist, we should create new one
FileManager::FileManager(const std::string &db_directory, int block_size) : 
    directory_name_(db_directory),block_size_(block_size) {
    std::string cmd;
    
    // check whether the file exist
    is_new_ = (access(directory_name_.c_str(), 0) != 0); /* non-exist */
    if(is_new_) { /* create */
        mkdir(directory_name_.c_str(), 0777);
    }
    // delete all .temp file in directory
    cmd = "rm -rf " + directory_name_ + "/*.temp";
    system(cmd.c_str());
}

// should consider three things
// 1. read past end of file 
// 2. io error
// 3. read less than a block
void FileManager::Read(const BlockId &block, Page &page) {
    std::lock_guard<std::mutex> lock(latch_); /* mutex lock! */
    std::string file_name = block.FileName();
    int block_number = block.BlockNum();
    int offset = block_number * block_size_;
    auto file_io = GetFile(file_name);
    uint32_t read_count;

    // call the GetFileSize func everytime?
    // TODO: we need to cache it
    if(offset > GetFileSize(file_name)) {
        std::cerr << "read past end of file" << std::endl;
        memset(&((*page.content())[0]),0,block_size_); 
        return;
    }
    // seek to the location you want to read, seekp and seekg work the same
    file_io->seekp(offset, std::ios::beg);
    file_io->read(&((*page.content())[0]), block_size_);
    if(file_io->bad()) {
        throw std::runtime_error("I/O error when read a block");
    }
    
    read_count = file_io->gcount();
    if(read_count < block_size_) {
        std::cerr << "read less than a block" << std::endl;
        file_io->clear();
        // set those to zero
        memset(&((*page.content())[0]) + read_count, 0, block_size_ - read_count);
        return;
    }
}

// Write can extend the file, 
// so it can write past the end of file
void FileManager::Write(const BlockId &block, Page &page) {
    std::lock_guard<std::mutex> lock(latch_); /* mutex lock! */
    std::string file_name = block.FileName();
    int block_number = block.BlockNum();
    int offset = block_number * block_size_;
    auto file_io = GetFile(file_name);
    
    file_io->seekp(offset, std::ios::beg);
    file_io->write(&((*page.content())[0]), block_size_);
    if(file_io->bad()) {
        throw std::runtime_error("I/O error when write a block");
    }
    // write immediately
    file_io->flush();
}

// By writing to next logical block to file that not belonging to us
// The OS will automatically complete the extension
BlockId FileManager::Append(const std::string &file_name) {
    std::lock_guard<std::mutex> lock(latch_); /* mutex lock! */
    auto file_io = GetFile(file_name);
    std::vector<char> empty_array(block_size_, 0);
    int next_block_num = Length(file_name);
    BlockId new_block_id(file_name, next_block_num);
    int offset = next_block_num * block_size_;

    file_io->seekp(offset, std::ios::beg);
    file_io->write(&(empty_array[0]), block_size_);
    if(file_io->bad()) {
        throw std::runtime_error("I/O error when append a block");
    }
    // write immediately
    file_io->flush();
    
    return new_block_id;
}

// 1. first, check whether the file has been opened
// 2. if the file exists but has not opened, we need to open it
// 3. if the file is not created, we also need to create it
// 4. remeber that put it in open_files_
std::shared_ptr<std::fstream> FileManager::GetFile(const std::string &file_name) {
    auto file_io = std::make_shared<std::fstream>();
    std::string file_path = directory_name_ + "/" + file_name;
    
    // search the hash-table
    if(open_files_.find(file_name) != open_files_.end()) {
        // the file has been created
        file_io = open_files_[file_name];
        if(file_io->is_open()) { /* file is being opened */
            return file_io;
        }
    }
    // file is not opened
    file_io->open(file_path, std::ios::binary | std::ios::in | std::ios::out);
    if(!file_io->is_open()) { /* the file is not created */
        file_io->clear();
        // create new file, use trunc to make sure to overwrite the original file content
        file_io->open(file_path, std::ios::binary | std::ios::in
            | std::ios::out | std::ios::trunc);
        // reopen the file with original mode
        file_io->close();
        file_io->open(file_path, std::ios::binary | std::ios::in | std::ios::out);
        
        if(!file_io->is_open()) { 
            throw std::runtime_error("can't open file " + file_name);
        }
    }
    // put it in open_files_
    open_files_[file_name] = file_io;
    return file_io;
}

int FileManager::GetFileSize(const std::string &file_name) {
    std::string file_path = directory_name_ + "/" + file_name;
    struct stat stat_buf;

    int success = stat(file_path.c_str(), &stat_buf);
    return success == 0 ? static_cast<int> (stat_buf.st_size) : -1;
}

} // namespace SimpleDB

#endif