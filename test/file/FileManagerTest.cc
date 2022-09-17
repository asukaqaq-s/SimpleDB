#include <iostream>
#include <unistd.h>
#include <cstdio>
#include "sys/stat.h"
#include <random>

#include "../../src/file/Blockid.cc"
#include "../../src/file/FileManager.cc"
#include "../../src/file/Page.cc"
#include "../../src/file/Page.h"
#include "../DebugFunction.cc"


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

    // test create file and append function
    for(int i = 0;i < 10;i ++) {
        auto block = fm.Append(file_name);
        printf("%s's size = %d\n", file_name.c_str(), GetFileSize(directory_path, file_name));
        block_array.push_back(block);
    }
    
    // test write and read function
    /* file -> write !*/
    for(int i = 0;i < 10;i ++) { /* Generates 10 pages of random content */
        SimpleDB::Page p(block_size);
        std::vector<char> byte_array(block_size - 4);
        
        for(int i = 0;i < block_size - 4;i ++) {
            byte_array[i] = u(e) + '0';
        }
        p.SetBytes(0, byte_array);
        // p.PrintPage(0);
        auto page_cotent = p.GetBytes(0); 
        for(int i = 0;i < block_size - 4;i ++) {
            Assert(byte_array[i] == page_cotent[i], "write to page fail");   
        }
        std::cout << page_cotent.size() << std::endl;
        page_array.push_back(p);
        // write to file
        fm.Write(block_array[i], p);
    }    

    /* file -> read ! */
    for(int i = 0;i < 10;i ++) {
        SimpleDB::Page p(block_size);
        fm.Read(block_array[i], p);

        Assert(*p.content() == *page_array[i].content(), "file->read fail");        
    }
    
    return 0;
}