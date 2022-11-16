#ifndef B_PLUS_TREE_DIRECTORY_PAGE_H
#define B_PLUS_TREE_DIRECTORY_PAGE_H

#include "index/btree/b_plus_tree_page.h"

namespace SimpleDB {

#define B_PLUS_TREE_DIRECTORY_PAGE_TYPE \
    BPlusTreeDirectoryPage<KeyType, ValueType, KeyComparator>


/**
* @brief
*
* --------------------------------------------
* | key 0 | value 1 | ..... | null | value n |
* --------------------------------------------
*/

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeDirectoryPage : public BPlusTreePage {
public:
    

    
    /**
    * @brief 
    * initialize directory page of b+tree
    * @param block_num id of page
    * @param parent_id id of parent page
    * @param max_size max count of pairs that can stored in this page
    */
    void Init(int block_num, int max_size = DIRECTORY_PAGE_MAX_SIZE, int parent_id = INVALID_BLOCK_NUM);



    /**
    * @brief 
    * get the key though the index
    * @param index 
    * @return KeyType 
    */
    KeyType KeyAt(int index) const;


    /**
    * @brief 
    * set the key though the index
    * @param index 
    * @param key 
    */
    void SetKeyAt(int index, const KeyType &key);



    /**
    * @brief 
    * find the index with the value equals to the input "value"
    * @param value 
    * @return index(>=0) corresponding to the value. or -1 when value doesn't exists
    */
    int ValueIndex(const ValueType &value) const;


    /**
    * @brief 
    * get the value though the index
    * @param index 
    * @return ValueType 
    */
    ValueType ValueAt(int index) const;



    /**
    * @brief 
    * find the child pointer which points to the page that contains input "key".
    * @param key 
    * @param comparator
    * @return block num which contains "key"
    */
    ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;



    /**
    * @brief init a new root node
    */
    void PopulateNewRoot(const KeyType &key, 
                         const ValueType &left_child,
                         const ValueType &right_child);
    

    
    
    /**
    * @brief 
    * insert new_key & new_value pair right before the first key with greater than new_key
    * we assume that 
    * @param new_key 
    * @param new_value 
    */
    void InsertNode(const KeyType &new_key, 
                    const ValueType &new_value, 
                    const KeyComparator &comparator);


    // split related

    /**
    * @brief 
    * remove half of key & value pairs from this page to recipient page
    * @param recipient 
    * @param bpm 
    */
    void MoveHalfTo(BPlusTreeDirectoryPage *recipient);


    void MoveLastToFirst(BPlusTreeDirectoryPage *recipient);


    void MoveFirstToLast(BPlusTreeDirectoryPage *recipient);



    // remove related

    /**
    * @brief 
    * remove the key & value pair in directory page corresponding to input index
    * @param index 
    */
    void RemoveAt(int index);


    /**
    * @brief 
    * Remove the only key & value pair in the directory page and return the value
    * @return ValueType 
    */
    ValueType RemoveAllChild();

    // merge related

    /**
    * @brief 
    * remove all of key & value pairs from current page to recipient. 
    * @param recipient 
    */
    void MoveAllTo(BPlusTreeDirectoryPage *recipient);



    static constexpr int DIRECTORY_PAGE_MAX_SIZE = (SIMPLEDB_BLOCK_SIZE - BPLUSTREE_HEADER_SIZE) / sizeof(MappingType);

    void PrintDir() const {
        std::cout << "parent = " << GetParentBlockNum() << "  "
                  << "blocknum = " << block_num_ << "  " 
                  << "size = " << GetSize() << "  " 
                  << "max_size = " << GetMaxSize() << std::endl;
        for (int i = 0; i < GetSize(); i++) {
            std::cout << "pair " << i << "  "
                      << "key = " << (*reinterpret_cast<const int*>(&array_[i].first)) << "   "
                      << "block_num = "<< (*reinterpret_cast<const int*>(&array_[i].second)) << std::endl;
        }
    }

public:
    // helper functions

    /**
    * @brief 
    * Copy entries into current page, starting from "items" and copy "size" entries.
    * We also need to set the parent_page of these new items to current page since their parent has changed now.
    * @param items 
    * @param size 
    */
    void CopyNFrom(MappingType *items, uint32_t size);


    void CopyLastFrom(const MappingType &item);

    void CopyFirstFrom(const MappingType &item);



    // elastic array
    MappingType array_[0];
};

}

#endif