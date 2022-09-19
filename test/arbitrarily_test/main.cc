#include <iostream>
#include <unistd.h>
#include <cstdio>
#include "sys/stat.h"
#include <random>
#include "sys/types.h"
#include <fcntl.h>
#include <fstream>
#include <sstream>


#include "file/Blockid.h"
#include "file/FileManager.h"
#include "file/Page.cc"
#include "file/Page.h"


int GetFileSize(const std::string &directory_name_, const std::string &file_name) {
    std::string file_path = directory_name_ + "/" + file_name;
    struct stat stat_buf;

    int success = stat(file_path.c_str(), &stat_buf);
    return success == 0 ? static_cast<int> (stat_buf.st_size) : -1;
}

int get(std::istream &x) {
    return  1;
}

struct node {
  int x;  
};

int main() {
    
    return 0;
}