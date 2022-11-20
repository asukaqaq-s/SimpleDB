#ifndef B_PLUS_TREE_ITERATOR_H
#define B_PLUS_TREE_ITERATOR_H

#include "index/btree/b_plus_tree_page.h"
#include "index/btree/b_plus_tree_bucket_page.h"
#include "index/btree/b_plus_tree_leaf_page.h"

namespace SimpleDB {


INDEX_TEMPLATE_ARGUMENTS
class BPlusTree;

#define BPLUSTREE_ITERATOR_TYPE BPlusTreeIterator<KeyType, ValueType, KeyComparator>


INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIterator {

    using LeafPage = BPlusTreeLeafPage<KeyType, RID, KeyComparator>;
    using BucketPage = BPlusTreeBucketPage<KeyType, RID, KeyComparator>;

public:
  
    BPlusTreeIterator(int curr_slot, int curr_block_num, Buffer *buffer,
                      BPlusTree<KeyType, ValueType, KeyComparator> *btree);
    BPlusTreeIterator() = default;
    ~BPlusTreeIterator();

    bool IsEnd();

    void Advance();

    const MappingType &operator*();

    bool operator==(const BPLUSTREE_ITERATOR_TYPE &itr);

    bool operator!=(const BPLUSTREE_ITERATOR_TYPE &itr);

    bool GetValue(std::vector<RID> *result);

    KeyType GetKey() const;


private:



    void Get();


    
    std::string index_file_name_;

    int curr_slot_;

    int curr_block_num_;

    // to lock or unlock
    Buffer *buffer_;

    // cache current leaf page
    LeafPage *leaf_page_{nullptr};

    // cache current item
    MappingType item_;

    BufferManager *buffer_manager_{nullptr};

    // cache bplus tree
    BPlusTree<KeyType, ValueType, KeyComparator> *btree_{nullptr};

};





} // namespace SimpleDB


#endif