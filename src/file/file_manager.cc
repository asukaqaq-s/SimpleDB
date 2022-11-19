#ifndef FILE_MANAGER_CC
#define FILE_MANAGER_CC

#include "file/file_manager.h"

#include <iostream>
#include <sys/stat.h>
#include <filesystem>
#include <unistd.h>
#include <cstring>
#include <time.h>

namespace SimpleDB {

// if the directory is not exist, we should create new one
FileManager::FileManager(const std::string &db_directory, int block_size) : 
    directory_name_(db_directory),block_size_(block_size) {
    std::string cmd;

    // check if the file exist, if not we should create a new one
    is_new_ = (access(directory_name_.c_str(), 0) != 0); 
    if(is_new_) {
        mkdir(directory_name_.c_str(), 0777);
    }

    // delete all .temp file in directory
    cmd = "rm -rf " + directory_name_ + "/*.temp";
    if (system(cmd.c_str()) == -1)  {
        SIMPLEDB_ASSERT(false, "create folder error");
    }
}



void FileManager::Read(const BlockId &block, Page *page) {
    // we should consider three things
    // 1. read past end of file 
    // 2. io error
    // 3. read less than a block


    std::lock_guard<std::mutex> lock(latch_); /* mutex lock! */
    std::string file_name = block.FileName();
    int block_number = block.BlockNum();
    int offset = block_number * block_size_;
    auto file_io = GetFile(file_name);
    int read_count;


    // call the GetFileSize func everytime?
    // TODO: we need to cache it
    if(offset > GetFileSize(file_name)) {
        std::cerr << "read past end of file" << std::endl;
        memset(page->GetRawDataPtr(), 0, block_size_); 
        return;
    }


    // seek to the location you want to read, seekp and seekg work the same
    file_io->seekp(offset);
    file_io->read(page->GetRawDataPtr(), block_size_);
    if(file_io->bad()) {
        throw std::runtime_error("I/O error when read a block");
    }

    // if read less than a block    
    read_count = file_io->gcount();
    if(read_count < block_size_) {
        std::cerr << "read less than a block" << std::endl;
        file_io->clear();
        // set those to zero
        memset(page->GetRawDataPtr() + read_count, 0, block_size_ - read_count);
        return;
    }
}


void FileManager::Write(const BlockId &block, Page *page) { 
    // Write can extend the file, 
    // so it can write past the end of file
    std::lock_guard<std::mutex> lock(latch_); 
    std::string file_name = block.FileName();
    int block_number = block.BlockNum();
    int offset = block_number * block_size_;
    auto file_io = GetFile(file_name);

    
    file_io->seekp(offset, std::ios::beg);
    file_io->write(page->GetRawDataPtr(), block_size_);
    if(file_io->bad()) {
        throw std::runtime_error("I/O error when write a block");
    }
    

    // write to disk immediately, we don't want data stored in buf of io_stream
    file_io->flush();
}


void FileManager::ReadLog(const std::string &log_name, int offset, Page &page) {
    std::lock_guard<std::mutex> lock(latch_);
    auto log_io = GetLogFile(log_name);
    int read_count;

    // check if read past  end of file
    if(offset > GetFileSize(log_name)) {
        std::cerr << "read past end of file" << std::endl;
        memset(&((*page.content())[0]),0,block_size_); 
        return;
    }


    // seek to the location you want to read, seekp and seekg work the same
    log_io->seekp(offset, std::ios::beg);
    log_io->read(&((*page.content())[0]), block_size_);
    if(log_io->bad()) {
        throw std::runtime_error("I/O error when read a log");
    }
    

    read_count = log_io->gcount();
    if(read_count < block_size_) {
        log_io->clear();
        memset(&((*page.content())[0]) + read_count, 0, block_size_ - read_count);
        return;
    }
}

void FileManager::WriteLog(const std::string &log_name, int size, Page &page) {
    if (size == 0) {
        return;
    }

    std::lock_guard<std::mutex> lock(latch_); /* mutex lock! */
    auto log_io = GetLogFile(log_name);
    log_io->write(&((*page.content())[0]), size);

    if(log_io->bad()) {
        throw std::runtime_error("I/O error when write a log");
    }


    // write to disk immediately
    log_io->flush();
}


BlockId FileManager::Append(const std::string &file_name) {
    // By writing to next logical block to file that not belonging to us
    // The OS will automatically complete the extension
    
    std::lock_guard<std::mutex> lock(latch_); /* mutex lock! */
    auto file_io = GetFile(file_name);
    std::vector<char> empty_array(block_size_, 0);
    int next_block_num = GetFileBlockNum(file_name);
    BlockId new_block_id(file_name, next_block_num);
    int offset = next_block_num * block_size_;


    // write a blank_block to disk
    file_io->seekp(offset, std::ios::beg);
    file_io->write(&(empty_array[0]), block_size_);
    if(file_io->bad()) {
        throw std::runtime_error("I/O error when append a block");
    }
    
    // write to disk immediately
    file_io->flush();
    return new_block_id;
}


std::fstream* FileManager::GetFile(const std::string &file_name) {
    // 1. first, check whether the file has been opened
    // 2. if the file exists but has not opened, we need to open it
    // 3. if the file is not exist, we also need to create it
    // 4. remeber that put it in open_files_ for caching it

    std::string file_path = directory_name_ + "/" + file_name;
    
    // search the hash-table
    if(open_files_.find(file_name) != open_files_.end()) {
        if(open_files_[file_name]->is_open()) { /* file is being opened */
            return open_files_[file_name].get();
        }
    }

    auto file_io = std::make_unique<std::fstream>();

    // file is not opened
    file_io->open(file_path, std::ios::binary | std::ios::in | std::ios::out);
    if(!file_io->is_open()) {
        // clear bad status
        file_io->clear();

        // create new file, use trunc to make sure to overwrite the original file content
        file_io->open(file_path, std::ios::binary | std::ios::in | 
                                 std::ios::out | std::ios::trunc);


        // reopen the file with original mode
        file_io->close();
        file_io->open(file_path, std::ios::binary | std::ios::in | std::ios::out);
        if(!file_io->is_open()) { 
            throw std::runtime_error("can't open file " + file_name);
        }
    }


    // put it in open_files_
    open_files_[file_name] = std::move(file_io);
    return open_files_[file_name].get();
}

std::fstream* FileManager::GetLogFile(const std::string &log_name) {
    std::string file_path = directory_name_ + "/" + log_name;
    
    // search the hash-table
    if(open_files_.find(log_name) != open_files_.end()) {
        // the file has been created
        if(open_files_[log_name]->is_open()) {
            return open_files_[log_name].get();
        }
    }
    
    auto log_io = std::make_unique<std::fstream>();

    // file is not opened
    // we should open this file with app mode
    log_io->open(file_path, std::ios::binary | std::ios::in | 
                            std::ios::out | std::ios::app);
    if(!log_io->is_open()) { /* the file is not created */
        // clear bad statu to ensure that normally operation
        log_io->clear();

        // create new file, use trunc to make sure to overwrite the original file content
        log_io->open(file_path, std::ios::binary | std::ios::in | 
                                std::ios::out | std::ios::trunc);
        
        // reopen the file with app mode to ensure sequential write
        log_io->close();
        log_io->open(file_path, std::ios::binary | std::ios::in | 
                                std::ios::out | std::ios::app);
        
        if(!log_io->is_open()) { 
            throw std::runtime_error("can't open log " + log_name);
        }
    }
    
    // put it in open_files_
    open_files_[log_name] = std::move(log_io);
    return open_files_[log_name].get();
}


int FileManager::GetFileSize(const std::string &file_name) {
    std::string file_path = directory_name_ + "/" + file_name;
    struct stat stat_buf;

    int success = stat(file_path.c_str(), &stat_buf);
    return success == 0 ? static_cast<int> (stat_buf.st_size) : 0;
}

void FileManager::SetFileSize(const std::string &file_name, int block_num) {
    std::string file_path = directory_name_ + "/" + file_name;
    int success = truncate(file_path.c_str(), block_num * block_size_);

    if (success == -1) {
        SIMPLEDB_ASSERT(false, "truncate file error");
    }
}

} // namespace SimpleDB

#endif