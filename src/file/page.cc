#ifndef PAGE_CC
#define PAGE_CC

#include "file/page.h"

#include <iostream>
#include <cstring>

namespace SimpleDB {

int Page::GetInt(int offset) const {
    char* page_offset;
    int res;
    
    if(offset + sizeof(int) > buffer_page_->size()) { 
        // overflow
        throw std::runtime_error("Page overflow when GetInt");
    }
    // no overflow
    page_offset = &(*buffer_page_)[offset];
    res = *(reinterpret_cast<int *>(page_offset));
    return res;
}

void Page::SetInt(int offset,int n) {
    char* page_offset;
    
    if(offset + sizeof(int) > buffer_page_->size()) { 
        // overflow
        throw std::runtime_error("Page overflow when SetInt");
    }
    // no overflow
    page_offset = &(*buffer_page_)[offset];
    *(reinterpret_cast<int *>(page_offset)) = n;
}

// a blob consists of a header(4 bytes, store byte-array's size) and a byte-array
// 1. read the blob's header,get the blob_size
// 2. check overflow
// 3. read the blob's byte_array, stores in vector<char>
std::vector<char> Page::GetBytes(int offset) const {
    char *page_offset_begin;
    char *page_offset_end;
    std::vector<char> byte_array;
    int blob_size = GetInt(offset);
    
    // the maximum of access address = sizeof(int) + blob_size + offset
    if(blob_size + sizeof(int) + offset > buffer_page_->size()) {
        // overflow
        // printf("blob_size = %d, offset = %d, buffer_page_->size = %d\n",
            // blob_size, offset, buffer_page_->size());
        throw std::runtime_error("Page overflow when GetBytes");
    }
    // no overflow
    page_offset_begin = &(*buffer_page_)[offset + sizeof(int)];
    page_offset_end = page_offset_begin + blob_size;
    
    byte_array.insert(byte_array.end(), page_offset_begin,
        page_offset_end);
    return byte_array;
}

// 1. set blob's header by calling SetInt func
// 2. prepare to set byte-array in blob and check overflow
// 3. set byte-array in blob
void Page::SetBytes(int offset, const std::vector<char> &byte_array) {
    char *page_offset;
    int blob_size = byte_array.size();
    
    // set the blob's header 
    SetInt(offset, blob_size);
    // check overflow
    if(blob_size + sizeof(int) + offset > buffer_page_->size()) {
        // overflow
        throw std::runtime_error("Page overflow when SetBytes");
    }
    // no overflow
    page_offset = &(*buffer_page_)[offset + sizeof(int)];

    std::memcpy(page_offset, &byte_array[0], blob_size);
}

// 1. read the byte-array by calling 'Getbytes'
// 2. convert byte-array to string
std::string Page::GetString(int offset) const {
    std::vector<char> byte_array = GetBytes(offset);
    std::string res(byte_array.begin(), byte_array.end());
    return res;
}

// 1. convert str to byte array
// 2. set byte-array by calling 'SetBytes'
void Page::SetString(int offset, const std::string &str) {
    std::vector<char> byte_array(str.begin(), str.end());
    SetBytes(offset, byte_array);
}

std::shared_ptr<std::vector<char>> Page::content() {
    return buffer_page_;
}

void Page::PrintPage(int mode) {
// mode 0: Getint
    printf("Print a Page :\n");
    if(mode == 0) {
        int size = GetInt(0);
        printf("size = %d\n", size);
        for(int i = 0;i < buffer_page_->size() - 4;i ++) {
            printf("%c", (*buffer_page_)[i]);
        }
        printf("\n");
    }
    else if(mode == 1) { /* log page */
        int boundary = GetInt(0);
        std::cout << "boundary = " << boundary << std::endl;
        for(int i = boundary;i < 4096;) {
            auto vec = GetBytes(i);
            for(auto t:vec)
                std::cout << t << std::flush;
            std::cout << std::endl;
            i += sizeof(int) + vec.size();
        }
    }
}

} // namespace SimpleDB


#endif