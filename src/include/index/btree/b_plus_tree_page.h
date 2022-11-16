#ifndef B_PLUS_TREE_PAGE_H
#define B_PLUS_TREE_PAGE_H

#include "buffer/buffer_manager.h"
#include "index/generic_key.h"
#include "file/page.h"

namespace SimpleDB {

#define INDEX_TEMPLATE_ARGUMENTS \
    template <typename KeyType, typename ValueType, typename KeyComparator>

// key-value pair in b+tree
#define MappingType std::pair<KeyType, ValueType>


/**
* @brief 
* Header part for B+Tree page. We inherit the basic header format from page.
* We didn't inherit Page class directly since we want to manipulate the data just like
* in memory data structure by reinterpreting data pointer to the BPlusTreePage.
* Header format:
* --------------------------------------------------------------------
* | LSN(4) | PageType(4) | BlockNum(4) | CurrentSize(4) | MaxSize(4) |
* --------------------------------------------------------------------
* | ParentBlockNum(4) |
* ---------------------
*/
class BPlusTreePage {


public:
    
    void SetLsn(lsn_t lsn) {
        lsn_ = lsn;
    }

    lsn_t GetLsn() const {
        return lsn_;
    }

    
    /**
    * @brief 
    * return whether current page is leaf page
    * @return
    */
    inline bool IsLeafPage() const {
        return page_type_ == PageType::BPLUS_TREE_LEAF_PAGE;
    }


    /**
    * @brief 
    * return whether current page is root page
    * @return true 
    * @return false 
    */
    bool IsRootPage() const {
        return parent_block_num_ == INVALID_BLOCK_NUM;
    }

    PageType GetPageType() const {
        return page_type_;
    }


    /**
    * @brief
    * set the type of current page
    * @param page_type 
    */
    void SetPageType(PageType page_type) {
        page_type_ = page_type;
    }


    /**
    * @brief
    * get size of current page
    * @return int 
    */
    int GetSize() const {
        return size_;
    }

    
    /**
    * @brief
    * set size of current page
    * @param size 
    */
    void SetSize(int size) {
        size_ = size;
    }

    
    /**
    * @brief 
    * increase the size of current page by amount
    * @param amount 
    */
    void IncreaseSize(int amount) {
        size_ += amount;
    }

    /**
    * @brief
    * get max page size.
    * when page size is greater than max size, we will trigger
    * a split on that page
    * @return int 
    */
    int GetMaxSize() const {
        return max_size_;
    }


    void SetMaxSize(int size) {
        max_size_ = size;
    }


    int GetBlockNum() const {
        return block_num_;
    }

    void SetBlockNum(int block_num) {
        block_num_ = block_num;
    }


    /**
    * @brief
    * get min page size. 
    * Generally, min page size == max page size / 2
    * @return uint32_t 
    */
    int GetMinSize() const {
        return (max_size_) / 2;
    }

    int GetParentBlockNum() const {
        return parent_block_num_;
    }

    void SetParentBlockNum(int parent_block_num) {
        parent_block_num_ = parent_block_num;
    }

protected:
    static_assert(sizeof(PageType) == 4);
    static constexpr uint32_t BPLUSTREE_HEADER_SIZE = 24;
    // member varibles that both internal page and leaf page
    // will share

    // page lsn
    lsn_t lsn_;

    // page type
    PageType page_type_;

    // block num to easy to op
    int block_num_;

    // count of pairs that stored in current page
    int size_;

    // max count of pairs that could be stored in current page
    int max_size_;
    
    // pointer to parent page
    int parent_block_num_;


};



} // namespace SimpleDB

#endif