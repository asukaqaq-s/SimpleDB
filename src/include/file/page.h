#ifndef PAGE_H
#define PAGE_H

#include "type/value.h"
#include "config/type.h"
#include "config/rw_latch.h"

#include <memory>
#include <vector>

namespace SimpleDB {


enum class PageType : int {
    DEFAULT_PAGE_TYPE = 0,
    TABLE_PAGE,
    HASH_BUCKET_PAGE,
    HASH_DIRECTORY_PAGE,
    BPLUS_TREE_LEAF_PAGE,
    BPLUS_TREE_BUCKET_PAGE,
    BPLUS_TREE_DIRECTORY_PAGE  
};


/**
* @brief 
* Page provides a wrapper for actual data pages being held in main memory.
* we can use a page object to bufferpool, log manager and so on.
*
* we can use a general page header to store some specefic informations
* general page type format:
* ------------------------
* | Lsn(4) | PageType(4) | 
* ------------------------
*/
class Page
{

public:

    /** 
    * @brief Constructor, Commly used to buffer manager
    * @param block_size 
    */
    Page(int block_size) {
        content_ = std::make_shared<std::vector<char>> (block_size);
    }
    

    Page(std::shared_ptr<std::vector<char>> &buffer_page)
        :content_(buffer_page) {}


    /**
    * @brief Get a char-value from page_[offset]
    * 
    * @param offset
    * @return the obtained bool-value
    */
    char GetByte(int offset) const;
    

    /**
    * @brief Set a char-value n to page_[offset]
    * 
    * @param offset
    * @param n the value to set
    */
    void SetByte(int offset, char n);

    
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
    * @brief Get a decimal from
    * 
    * @param offset
    */
    double GetDec(int offset) const;

    /**
    * @brief Set a decimal to page_[offset]
    * 
    * @param offset
    * @param str 
    */
    void SetDec(int offset, double decimal);

    /**
    * @brief a general and more convenient way to read
    * data to page
    * @param offset
    * @param constant 
    */
    void SetValue(int offset, Value val);

    /**
    * @brief a general and more convenient way to write
    * data from page
    * @param offset
    * @param constant
    */
    Value GetValue(int offset,TypeID type);

    /**
    * @brief calculates the maximum size of blobs
    * depends on which characters, UTF, ASCII ...
    * 
    * @param strlen The number of characters inputed
    * @return sizeof(int) + (strlen * The byte of each character)
    */
    static int MaxLength(int strlen) { return sizeof(int) + strlen; }
    

    /**
    * @brief page's content,usually 4kb size. 
    * 
    * @return shard_ptr, jusk lisk a vector<char>* 
    */
    std::shared_ptr<std::vector<char>> content();

    
    int GetSize() { return content_->size(); }

    
    char *GetRawDataPtr() { return &(*content_)[0]; }


    void ZeroPage() { content_ = std::make_shared<std::vector<char>>(content_->size(), 0); }

public: // for recovery manager, we can use lsn to get more information

    static constexpr int LSN_OFFSET = 0;
    static constexpr int PAGE_TYPE_OFFSET = sizeof(int);
    static constexpr int PAGE_HEADER_SIZE = PAGE_TYPE_OFFSET + sizeof(int);

    inline void SetLsn(lsn_t lsn) {
        SIMPLEDB_ASSERT(content_->size() >= PAGE_HEADER_SIZE, "error");
        SetInt(LSN_OFFSET, lsn);
    }

    inline lsn_t GetLsn() const {
        SIMPLEDB_ASSERT(content_->size() >= PAGE_HEADER_SIZE, "error");
        return GetInt(LSN_OFFSET);
    }

    inline void SetPageType(PageType type) {
        SIMPLEDB_ASSERT(content_->size() >= PAGE_HEADER_SIZE, "error");
        SetInt(PAGE_TYPE_OFFSET, static_cast<int>(type));
    }

    inline PageType GetPageType() const {
        SIMPLEDB_ASSERT(content_->size() >= PAGE_HEADER_SIZE, "error");
        return static_cast<PageType>(GetInt(PAGE_TYPE_OFFSET));
    }
    

    
private:

    // Page's content, stored in heap
    std::shared_ptr<std::vector<char>> content_;
};
}
#endif