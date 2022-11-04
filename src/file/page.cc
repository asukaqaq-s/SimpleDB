#ifndef PAGE_CC
#define PAGE_CC

#include "file/page.h"

#include <iostream>
#include <cstring>

namespace SimpleDB {

int Page::GetInt(int offset) const {
    char* page_offset;
    int res;
    
    if(offset + sizeof(int) > content_->size()) { 
        // overflow
        throw std::runtime_error("Page overflow when GetInt");
    }
    // no overflow
    page_offset = &(*content_)[offset];
    res = *(reinterpret_cast<int *>(page_offset));
    return res;
}

void Page::SetInt(int offset,int n) {
    char* page_offset;
    
    if(offset + sizeof(int) > content_->size()) { 
        // overflow
        throw std::runtime_error("Page overflow when SetInt");
    }
    // no overflow
    page_offset = &(*content_)[offset];
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
    if(blob_size + sizeof(int) + offset > content_->size()) {
        // overflow
        // printf("blob_size = %d, offset = %d, buffer_page_->size = %d\n",
            // blob_size, offset, buffer_page_->size());
        throw std::runtime_error("Page overflow when GetBytes");
    }
    // no overflow
    page_offset_begin = &(*content_)[offset + sizeof(int)];
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
    if(blob_size + sizeof(int) + offset > content_->size()) {
        // overflow
        throw std::runtime_error("Page overflow when SetBytes");
    }
    // no overflow
    page_offset = &(*content_)[offset + sizeof(int)];

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

double Page::GetDec(int offset) const {
    char* page_offset;
    double res;
    
    if(offset + sizeof(double) > content_->size()) { 
        // overflow
        throw std::runtime_error("Page overflow when GetInt");
    }
    // no overflow
    page_offset = &(*content_)[offset];
    res = *(reinterpret_cast<double *>(page_offset));
    return res;
}

void Page::SetDec(int offset,double n) {
    char* page_offset;
    
    if(offset + sizeof(int) > content_->size()) { 
        // overflow
        throw std::runtime_error("Page overflow when SetInt");
    }
    // no overflow
    page_offset = &(*content_)[offset];
    *(reinterpret_cast<double *>(page_offset)) = n;
}

void Page::SetValue(int offset, Value val) {
    int type = val.GetTypeID();
    
    switch(type) {
    
    // since page object doesn't know the offset of varchar_data
    // so we don't distinguish with char and varchar.
    case TypeID::CHAR:
    case TypeID::VARCHAR:
        SetString(offset, val.AsString());
        break;
        
    case TypeID::DECIMAL:
        SetDec(offset, val.AsDouble());
        break;
    
    case TypeID::INTEGER:
        SetInt(offset, val.AsInt());
        break;
    
    default:
        SIMPLEDB_ASSERT(false, "");
        break;
    }
}

Value Page::GetValue(int offset,TypeID type) {
    switch(type) {
    
    case TypeID::CHAR:
        return Value(GetString(offset), TypeID::CHAR);
        break;

    case TypeID::VARCHAR:
        return Value(GetString(offset), TypeID::VARCHAR);
        break;
        
    case TypeID::DECIMAL:
        return Value(GetDec(offset));
        break;
    
    case TypeID::INTEGER:
        return Value(GetInt(offset));
        break;
    
    default:
        SIMPLEDB_ASSERT(false, "");
        break;
    }
}


std::shared_ptr<std::vector<char>> Page::content() {
    return content_;
}


} // namespace SimpleDB


#endif