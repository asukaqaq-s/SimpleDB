#ifndef BPLUS_TREE_DIRECTORY_PAGE_H
#define BPLUS_TREE_DIRECTORY_PAGE_H

#include "buffer/buffer.h"
#include "buffer/buffer_manager.h"
#include "record/schema.h"
#include "record/rid.h"


namespace SimpleDB {

/**
* @brief
* Header format:
* since b+tree iterator don't need to access directory page, so nextblocknum
* is not necessary.we can use ParentBlock to get the sibling page of this page.
* ---------------------------------------------------------------------
* | LSN(4) | PageType(4) | CurrentSize(4) | MaxSize(4) | TupleSize(4) | 
* ---------------------------------------------------------------------
* | TypeID(4) | ParentBlock(4) |
* ------------------------------
* Directory Page format:
* ----------------------------------------------------
* | Header | kv-pair 1 | kv-pair 2 | kv-pair 3 | ...
* ----------------------------------------------------
* It looks a lot like Leaf_Page, but except for a point.
* for example, assume that we create a new directory page and insert a key(named dir_key) 
* into it, there are two pointers in directory page, one points to a block with all keys 
* less than dir_key and the other points to the block with all keys greater_equal than dir_key.
* 
* below is an illustration:
* directory page: size = 2 
*         ----------------------------------------------------------------------
*         | pointer(value) 0 | key 0: null | pointer(value) 1 | key 1: dir_key |
*         ----------------------------------------------------------------------
*           /                                               \ 
*          /                                                 \ 
*         /                                                   \
*  left child( leaf or directory)                        right child( leaf or directory) 
* 
* note that: key 0 is always invalid.         
*/
class BPlusTreeDirectoryPage : public Buffer {


public:

    using KeyType = Value;
    using ValueType = int;

    using KVPair = std::pair<Value, int>;


    BPlusTreeDirectoryPage() = delete;

    
    /**
    * @brief 
    * initialize directory page of b+tree
    * @param key_size the size of key
    * @param type typeid of key
    * @param parent_id id of parent page
    */
    void Init(int key_size, TypeID type, int parent_num = INVALID_BLOCK_NUM);


    /**
    * @brief 
    * get the key through the index
    * @param index 
    * @return Value 
    */
    Value KeyAt(int index) const;


    /**
    * @brief 
    * set the key through the index
    * @param index 
    * @param key 
    */
    void SetKeyAt(int index, const Value &key);


    /**
    * @brief 
    * find the index with the value equals to the input "value"
    * @param value 
    * @return index(>=0) corresponding to the value. or -1 when value doesn't exists
    */
    int ValueIndex(const int &value) const;

    
    /**
    * @brief 
    * get the value through the index
    * @param index 
    * @return int 
    */
    int ValueAt(int index) const;


    /**
    * @brief
    * set the value through the index
    */
    void SetValueAt(int index, const int &num);


    /**
    * @brief 
    * find the child pointer which points to the page that contains input "key".
    * Start the search from the second key(the first key should always be invalid)
    * different from leaf page, key of directory page is always unique
    * @param key 
    * @param comparator 
    * @return int 
    */
    int GetValue(const Value &key) const;


    // insertion related


    /**
    * @brief 
    * populate new root page with old_value + new_key & new_value.
    * This method should be called whenever we created a new root page
    * @param old_value 
    * @param new_key 
    * @param new_value 
    */
    void PopulateNewRoot(const int &old_value, 
                         const Value &new_key, 
                         const int &new_value);


    /**
    * @brief 
    * insert new_key & new_value pair right after the pair with it's value equals old_value
    * @param old_value 
    * @param new_key 
    * @param new_value 
    * @return uint32_t new size after insertion
    */
    uint32_t InsertNodeAfter(const int &old_value, 
                             const Value &new_key, 
                             const int &new_value);


    // remove related


    /**
    * @brief 
    * remove the key & value pair in internal page corresponding to input index
    * @param index 
    */
    void Remove(int index);


    /**
     * @brief 
     * Remove the only key & value pair in the internal page and return the value
     * @return int 
     */
    int RemoveAndReturnOnlyChild();

    // split related


    /**
    * @brief 
    * remove half of key & value pairs from this page to recipient page
    * @param recipient 
    * @param bpm 
    */
    void MoveHalfTo(BPlusTreeDirectoryPage *recipient, BufferManager *bpm);


    // merge related

    /**
    * @brief 
    * remove all of key & value pairs from current page to recipient. middle key is the separation key
    * that we should also added to the recipient to maintain the invariant. Bufferpool is also needed to
    * do the reparent
    * @param recipient 
    * @param middle_key 
    * @param bpm 
    */
    void MoveAllTo(BPlusTreeDirectoryPage *recipient, 
                   BufferManager *bpm);

    
    // redistributed related


    /**
    * @brief 
    * move the first key & value pair from current page to the tail of recipient page. We need bufferpool 
    * to do the reparent
    * @param recipient 
    * @param middle_key 
    * @param bpm 
    */
    void MoveFirstToEndOf(BPlusTreeDirectoryPage *recipient, 
                          BufferManager *bpm);


    /**
    * @brief 
    * remove the last key & value pair from this page to the head of recipient page.
    * @param recipient 
    * @param middle_key 
    * @param bpm 
    */
    void MoveLastToFrontOf(BPlusTreeDirectoryPage *recipient, 
                           BufferManager *bpm);


    void PrintDirectory() {
        printf("pageid: %d parent: %d size: %d\n", GetBlockID().BlockNum(), GetParentBlockNum(), GetSize());
        fflush(stdout);
        for (int i = 0; i < GetSize(); i++) {
            std::cout << KeyAt(i).to_string() << "   " << ValueAt(i)<<std::endl;
        }
    }

    inline void SetSize(int size) {
        data_->SetInt(CURRENT_SIZE_OFFSET, size);
    }

    inline int GetSize() const {
        return data_->GetInt(CURRENT_SIZE_OFFSET);
    }

public:
    // helper functions


    /**
    * @brief 
    * Copy "size" items starting from "items" to current page
    * @param items 
    * @param size 
    */
    void CopyNFrom(BPlusTreeDirectoryPage* sender, 
                   int begin, int size, BufferManager *bfm);


    /**
    * @brief 
    * Copy the item into the end of current page
    * @param item 
    */
    void CopyLastFrom(const KVPair &item, BufferManager *bfm);


    /**
    * @brief 
    * Copy the item into the head of current page
    * @param item 
    */
    void CopyFirstFrom(const KVPair &item, BufferManager *bfm);


private:

    static constexpr int LSN_OFFSET = 0;
    static constexpr int PAGE_TYPE_OFFSET = LSN_OFFSET + sizeof(lsn_t);
    static constexpr int CURRENT_SIZE_OFFSET = PAGE_TYPE_OFFSET + sizeof(int);
    static constexpr int MAX_SIZE_OFFSET = CURRENT_SIZE_OFFSET + sizeof(int);
    static constexpr int TUPLE_SIZE_OFFSET = MAX_SIZE_OFFSET + sizeof(int);
    static constexpr int TYPE_ID_OFFSET = TUPLE_SIZE_OFFSET + sizeof(int);
    static constexpr int PARENT_BLOCK_NUM_OFFSET = TYPE_ID_OFFSET + sizeof(int);
    static constexpr int DIRECTORY_PAGE_HEADER_SIZE = PARENT_BLOCK_NUM_OFFSET + sizeof(int);

    
    inline void SetLsn(lsn_t lsn) {
        data_->SetLsn(lsn);
    }

    inline lsn_t GetLsn() const {
       return data_->GetLsn();
    }

    inline void SetPageType(PageType type) {
        data_->SetPageType(type);
    }

    inline PageType GetPageType() const {
        return data_->GetPageType();
    }

    inline void SetMaxSize(int size) {
        data_->SetInt(MAX_SIZE_OFFSET, size);
    }

    inline int GetMaxSize() const {
        return data_->GetInt(MAX_SIZE_OFFSET);
    }

    inline void SetTupleSize(int size) {
        data_->SetInt(TUPLE_SIZE_OFFSET, size);
    }

    inline int GetTupleSize() const {
        return data_->GetInt(TUPLE_SIZE_OFFSET);
    }

    inline void SetTypeID(TypeID type) {
        data_->SetInt(TYPE_ID_OFFSET, static_cast<int>(type));
    }

    inline TypeID GetTypeID() const {
        return static_cast<TypeID> (data_->GetInt(TYPE_ID_OFFSET));
    }

    inline void SetParentBlockNum(int block_num) {
        data_->SetInt(PARENT_BLOCK_NUM_OFFSET, block_num);
    } 

    inline int GetParentBlockNum() const {
        return data_->GetInt(PARENT_BLOCK_NUM_OFFSET);
    } 

};


} // namespace SimpleDB


#endif