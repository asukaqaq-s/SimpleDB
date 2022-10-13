#ifndef STREAM_READER_H
#define STREAM_READER_H

#include <istream>
#include <vector>


namespace SimpleDB {

/**
* @brief this class is used by dealing input stream
*/
class StreamReader {

public:

    /**
    * @brief create a stream_reader object with a read buf
    */
    StreamReader(std::istream &ifs);

    /**
    * @brief read a char in buf and move one byte backward
    * if can not read, we should return -1 means that read fail
    * @return -1 if read fial, else return char value
    */
    int Read();

    /**
    * @brief modify the current pos byte to val
    * and move one byte forward
    */
    void Unread(int val);

private:

    int cur_pos_;
    
    std::vector<char> buf_;

};


} // namespace SimpleDB

#endif