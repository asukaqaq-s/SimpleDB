#include <iostream>
#include <unistd.h>
#include <cstdio>
#include "sys/stat.h"
#include <random>
#include "sys/types.h"
#include <fcntl.h>

#include "../src/file/Blockid.cc"
#include "../src/file/FileManager.cc"
#include "../src/file/Page.cc"
#include "../src/file/Page.h"
#include "DebugFunction.cc"


int GetFileSize(const std::string &directory_name_, const std::string &file_name) {
    std::string file_path = directory_name_ + "/" + file_name;
    struct stat stat_buf;

    int success = stat(file_path.c_str(), &stat_buf);
    return success == 0 ? static_cast<int> (stat_buf.st_size) : -1;
}

int main() {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    SimpleDB::FileManager fm(directory_path, block_size);
    std::vector<SimpleDB::BlockId> block_array;
    std::vector<SimpleDB::Page> page_array;
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

/*
    SimpleDB::BlockId blk("test1.txt", 1);
    std::vector<char> hh(100);
    SimpleDB::Page h(4096);O_CREAT
    h.SetBytes(0, hh);
    fm.Write(blk, h);
  */  
    // 
    char buf1[10] = "qaq1";
    std::fstream file_io;
    std::string file_path = local_path + "/" + file_name;
    file_io.open(file_path.c_str(), std::ios::in | std::ios::out | std::ios::trunc | std::ios::binary);
    file_io.write(buf1, 10);
    
    std::cout << GetFileSize(local_path.c_str(), "test1.txt");
    
    return 0;
}