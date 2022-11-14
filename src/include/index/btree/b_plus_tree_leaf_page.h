#ifndef BPLUS_TREE_LEAF_PAGE_H
#define BPLUS_TREE_LEAF_PAGE_H

#include "buffer/buffer.h"
#include "config/config.h"
#include "config/type.h"
#include "record/rid.h"
#include "concurrency/transaction.h"


namespace SimpleDB {



/**
* @brief
* Header format:
* ---------------------------------------------------------------------
* | LSN(4) | PageType(4) | CurrentSize(4) | MaxSize(4) | TupleSize(4) | 
* ---------------------------------------------------------------------
* | TypeID(4) | ParentBlock(4) | NextBlockNum(4) |
* ------------------------------------------------
* Leaf Page format:
* ----------------------------------------------------
* | Header | kv-pair 1 | kv-pair 2 | kv-pair 3 | ...
* ----------------------------------------------------
*/
class BPlusTreeLeafPage : public Buffer {

public:

    using KVPair = std::pair<Value, RID>;


    BPlusTreeLeafPage() = delete;
    
    /**
    * @brief 
    * Initialize B+tree leaf page. we should call it whenever we create a new leaf node page
    * @param block_num
    * @param parent_id 
    * @param max_size 
    */
    void Init(int key_size, TypeID type, int parent_num = INVALID_BLOCK_NUM);



    /**
    * @brief get key through index
    * @param index 
    * @return key 
    */
    Value KeyAt(int index) const;


    /**
    * @brief 
    * get value through index
    * @param index 
    * @return rid
    */
    RID ValueAt(int index) const;


    /**
    * @brief 
    * find the first index i so that array[i].first >= key.
    * You can regard it as lower_bound in c++ std library.
    * @param key 
    * @return index i
    */
    int KeyIndexGreaterEqual(const Value &key) const;


    /**
    * @brief 
    * find the first index i so that array[i].first > key.
    * You can regard it as upper_bound in c++ std library.
    * @param key 
    * @param comparator 
    * @return int 
    */
    int KeyIndexGreaterThan(const Value &key) const;


    /**
    * @brief
    * get the key & value pair through "index"
    * @param index 
    * @return KVPAIR
    */
    KVPair GetItem(int index);


    /**
    * @brief
    * set the key & value pair through "index"
    * @param index 
    * @return KVPAIR 
    */
    void SetItem(int index, const KVPair &kv);


    /**
    * @brief 
    * Insert key & value pair into leaf page ordered by key
    * assume that key don't exist in this leaf before.
    * @param key 
    * @param value 
    * @return page size after insertion
    */
    int Insert(const Value &key, const RID &value);


    /**
    * @brief 
    * for the given key, check to see whether it exists in the leaf page and stores its
    * corresponding value in a RID array.
    * if this value of page 
    * @param key 
    * @param value 
    * @param comparator 
    * @return whether key exists
    */
    bool GetValue(const Value &key, 
                  std::vector<RID> *result, 
                  int *overflow_bucket_num) const;


    /**
    * @brief 
    * find the key & value pair corresponding "key" and delete it.
    * @param key 
    * @param comparator 
    * @return whether deletion is performed
    */
    bool Remove(const Value &key, const RID &rid);


    /**
    * @brief 
    * Remove half of key & value pairs from current page to recipient page
    * @param recipient 
    */
    void MoveHalfTo(BPlusTreeLeafPage *recipient);


    /**
    * @brief 
    * Remove all of key & value pairs from current page to recipient page
    * @param recipient 
    */
    void MoveAllTo(BPlusTreeLeafPage *recipient);


    /**
    * @brief 
    * Remove the first key & value pair from current page to the end of recipient page
    * @param recipient 
    */
    void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);


    /**
    * @brief 
    * Remove the last key & value pair from current page to the head of recipient page
    * @param recipient 
    */
    void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

    /**
    * @brief Get a number which [CurrentSize + 1]
    */
    int GetHalfCurrSize();


    // for debug purpose
    void PrintLeaf() {
        printf("pageid: %d parent: %d size: %d\n", GetBlockID().BlockNum(), GetParentBlockNum(), GetSize());
        fflush(stdout);
        for (int i = 0; i < GetSize(); i++) {
            std::cout << GetItem(i).first.to_string() << "  " << GetItem(i).second.ToString() << std::endl;
        }
        std::cout << std::endl;
    }


        /**
    * @brief set next leaf page's block number
    */
    inline void SetNextBlockNum(int block_num) {
        data_->SetInt(NEXT_BLOCK_NUM_OFFSET, block_num);
    } 


    /**
    * @brief get next leaf page's block number 
    */
    inline int GetNextBlockNum() const {
        return data_->GetInt(NEXT_BLOCK_NUM_OFFSET);
    } 

    inline void SetParentBlockNum(int block_num) {
        data_->SetInt(PARENT_BLOCK_NUM_OFFSET, block_num);
    } 

    inline int GetParentBlockNum() const {
        return data_->GetInt(PARENT_BLOCK_NUM_OFFSET);
    } 


public:


    /**
    * @brief 
    * Copy "size" items starting from "items" to current page
    * @param items 
    * @param size 
    */
    void CopyNFrom(char *items, int size);


    /**
    * @brief 
    * Copy the item into the end of current page
    * @param item 
    */
    void CopyLastFrom(const KVPair &item);


    /**
    * @brief 
    * Copy the item into the head of current page
    * @param item 
    */
    void CopyFirstFrom(const KVPair &item);


private: // check value to deal with duplicated key

    static constexpr int POINT_TO_BUCKET_CHAIN = -1;


    bool ValueIsRID(const RID &value) const;
    

    bool ValueIsBucket(const RID &value) const;



private:
    

    static constexpr int LSN_OFFSET = 0;
    static constexpr int PAGE_TYPE_OFFSET = LSN_OFFSET + sizeof(lsn_t);
    static constexpr int CURRENT_SIZE_OFFSET = PAGE_TYPE_OFFSET + sizeof(int);
    static constexpr int MAX_SIZE_OFFSET = CURRENT_SIZE_OFFSET + sizeof(int);
    static constexpr int TUPLE_SIZE_OFFSET = MAX_SIZE_OFFSET + sizeof(int);
    static constexpr int TYPE_ID_OFFSET = TUPLE_SIZE_OFFSET + sizeof(int);
    static constexpr int PARENT_BLOCK_NUM_OFFSET = TYPE_ID_OFFSET + sizeof(int);
    static constexpr int NEXT_BLOCK_NUM_OFFSET = PARENT_BLOCK_NUM_OFFSET + sizeof(int);
    static constexpr int LEAF_PAGE_HEADER_SIZE = NEXT_BLOCK_NUM_OFFSET + sizeof(int);

    
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

    inline void SetSize(int size) {
        data_->SetInt(CURRENT_SIZE_OFFSET, size);
    }

    inline int GetSize() const {
        return data_->GetInt(CURRENT_SIZE_OFFSET);
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






};


} // namespace SimpleDB

#endif