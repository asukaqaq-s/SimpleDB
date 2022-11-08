#ifndef STREAM_READER_H
#define STREAM_READER_H

#include <istream>
#include <vector>


namespace SimpleDB {

/**
* @brief this class is used to solve input stream
*/
class StreamReader {

public:

    /**
    * @brief create a stream_reader object with a read buffer
    */
    StreamReader(std::istream &ifs);

    /**
    * @brief read a char in buf and move one byte backward
    * if can not read, we should return -1 means that read fail
    * @return -1 if read fail, else return value
    */
    int Read();

    /**
    * @brief modify byte of the current position and rollback one byte
    */
    void Unread(int val);

private:

    // location of the currently read file
    int cur_pos_;
    
    // a buffer which cache input stream
    std::vector<char> buf_;

};


} // namespace SimpleDB

#endif