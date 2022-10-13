#ifndef STREAM_READER_CC
#define STREAM_READER_CC

#include "parse/stream_reader.h"
#include "config/macro.h"

#include <iostream>

namespace SimpleDB {

StreamReader::StreamReader(std::istream &ifs) {
    ifs.seekg(0, std::ios::end);
    int len = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    buf_.resize(len);
    cur_pos_ = 0;
    ifs.read(&(buf_[0]), len);
}

int StreamReader::Read() {
    
    if (cur_pos_ == static_cast<int>(buf_.size())) {
        return -1;
    }
    int res = static_cast<int>(buf_[cur_pos_++]);
    return res;
}

void StreamReader::Unread(int val) {
    
    if (cur_pos_ == 0) {
        SIMPLEDB_ASSERT(false, "can not move");
    }
    
    char res = static_cast<char> (val);
    // we need --int instead int--
    // because val is the previous char value
    buf_[--cur_pos_] = res;
}


} // namespace SimpleDB

#endif
