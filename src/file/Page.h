#ifndef PAGE_H
#define PAGE_H

#include <memory>
#include <vector>

namespace SimpleDB {

/**
* @brief 
* a page corresponds to a disk-block
* a page is just 8b-size, and have a pointer that points to Page's content in heap 
*/
class Page
{

public:

    /** 
    * @brief Constructor
    *   Commly used to buffer manager
    * @param block_size 
    */
    Page(int block_size) {
        buffer_page_ = 
            std::make_shared<std::vector<char>> (block_size);
    }
    
    /**
    * @brief copy-constructor,
    *   will Increasing the reference count
    *   Commly used to log manager
    * @param buffer_page
    */
    Page(std::shared_ptr<std::vector<char>> &buffer_page)
        :buffer_page_(buffer_page) {}
    
    /**
    * @brief Get a int-value from page_[offset]
    * 
    * @param offset
    * @return the obtained int-value
    */
    int GetInt(int offset) const;

    /**
    * @brief Set a int-value n to page_[offset]
    * 
    * @param offset
    * @param n the value to set
    */
    void SetInt(int offset, int n);

    /**
    * @brief Get a blob from page_[offset]
    * A blob consists of a header and many others bytes(called byte-array)
    * 
    * @param offset 
    * @return the obtained byte-array
    */
    std::vector<char> GetBytes(int offset) const;

    /**
    * @brief Set a byte-array n to page_[offset]
    * set the offset = byte_array.size()
    * set the blob's content = byte_array
    * @param offset
    * @param byte_array 
    */
    void SetBytes(int offset, const std::vector<char> &byte_array);

    /**
    * @brief Get a string from page_[offset]
    * 
    * @param offset
    * @return the obtained string
    */
    std::string GetString(int offset) const;

    /**
    * @brief Set a string to page_[offset]
    * 
    * @param offset
    * @param str 
    */
    void SetString(int offset, const std::string &str);

    /**
    * @brief calculates the maximum size of blobs
    *   depends on which characters, UTF, ASCII ...
    * 
    * @param strlen The number of characters inputed
    * @return sizeof(int) + (strlen * The byte of each character)
    */
    static int MaxLength(int strlen);
    
    /**
    * @brief page's content,usually 4kb size. 
    * 
    * @return shard_ptr, jusk lisk a vector<char>* 
    */
    std::shared_ptr<std::vector<char>> content();

    /**
    * @brief for debugging purpose
    * 
    */
    void PrintPage(int mode);
private:
    // Page's content, stored in heap
    std::shared_ptr<std::vector<char>> buffer_page_;
};
}
#endif