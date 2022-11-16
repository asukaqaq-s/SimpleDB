#ifndef B_PLUS_TREE_LEAF_PAGE_H
#define B_PLUS_TREE_LEAF_PAGE_H

#include "index/btree/b_plus_tree_page.h"

#define B_PLUS_TREE_LEAF_PAGE_TYPE \
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

namespace SimpleDB {

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {



public:


    /**
    * @brief 
    * Initialize B+tree leaf page. we should call it whenever we create a new leaf node page
    * @param block_num 
    * @param parent_block_num 
    */
    void Init(int block_num, int max_size = LEAF_PAGE_MAX_SIZE, int parent_block_num = INVALID_BLOCK_NUM);

    
    /**
    * @brief
    * get the pointer pointing to next sibling page
    * @return int 
    */
    int GetNextBlockNum() const;

    
    /**
    * @brief
    * set the pointer pointing to next sibling page
    * @param next_block_num 
    */
    void SetNextBlockNum(int next_block_num);

    
    /**
    * @brief 
    * get key though index
    * @param index 
    * @return KeyType 
    */
    KeyType KeyAt(int index) const;

    
    /**
    * @brief 
    * get value though index
    * @param index 
    * @return ValueType
    */
    ValueType ValueAt(int index) const;


    void SetKeyAt(int index, KeyType key);

    void SetValueAt(int index, ValueType value);

    
    /**
    * @brief 
    * find the first index i so that array[i].first >= key.
    * You can regard it as lower_bound in c++ std library.
    * @param key 
    * @param comparator 
    * @return index i
    */
    int KeyIndexGreaterEqual(const KeyType &key, const KeyComparator &comparator) const;


    /**
    * @brief 
    * find the first index i so that array[i].first > key.
    * You can regard it as upper_bound in c++ std library.
    * @param key 
    * @param comparator 
    * @return int 
    */
    int KeyIndexGreaterThan(const KeyType &key, const KeyComparator &comparator) const;

    
    /**
    * @brief
    * get the key & value pair though "index"
    * @param index 
    * @return const MappingType& 
    */
    const MappingType &GetItem(int index);


    /**
    * @brief 
    * Insert key & value pair into leaf page ordered by key
    * @param key 
    * @param value 
    * @param comparator 
    * @return true if success
    */
    bool Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);


    /**
    * @brief 
    * for the given key, check to see whether it exists in the leaf page and stores its
    * corresponding value in "value"
    * @param key 
    * @param value 
    * @param comparator 
    * @return whether key exists
    */
    bool Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const;


    /**
    * @brief 
    * find the key & value pair corresponding "key" and delete it.
    * @param key 
    * @param comparator 
    * @return whether deletion is performed
    */
    bool Remove(const KeyType &key, const KeyComparator &comparator);


    /**
    * @brief 
    * Remove ahlf of key & value pairs from current page to recipient page
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


    static constexpr int LEAF_PAGE_HEADER_SIZE = BPLUSTREE_HEADER_SIZE + sizeof(int);
    static constexpr int LEAF_PAGE_MAX_SIZE = (SIMPLEDB_BLOCK_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType);

    // for debug purpose
    void PrintLeaf() {
        std::cout << "parent = " << GetParentBlockNum() << "  "
                  << "blocknum = " << block_num_ << "  " 
                  << "size = " << GetSize() << "  " 
                  << "max_size = " << GetMaxSize() << std::endl;
        for (int i = 0; i < GetSize(); i++) {
            std::cout << (*reinterpret_cast<int*>(&array_[i].first)) << "   "
                      << (reinterpret_cast<RID*>(&array_[i].second)->ToString()) << std::endl;
        }
    }

public:
    
    
    
    /**
    * @brief 
    * Copy "size" items starting from "items" to current page
    * @param items 
    * @param size 
    */
    void CopyNFrom(MappingType *items, int size);


    /**
    * @brief 
    * Copy the item into the end of current page
    * @param item 
    */
    void CopyLastFrom(const MappingType &item);


    /**
    * @brief 
    * Copy the item into the head of current page
    * @param item 
    */
    void CopyFirstFrom(const MappingType &item);

    
    // pointer pointing to next sibling page
    int next_block_num_{0};


    // elastic array. storing key-value pairs
    MappingType array_[0];
};

}

#endif