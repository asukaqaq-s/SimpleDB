#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include "record/schema.h"
#include "index/btree/b_plus_tree_bucket_page.h"
#include "index/btree/b_plus_tree_directory_page.h"
#include "index/btree/b_plus_tree_leaf_page.h"
#include "index/btree/b_plus_tree_iterator.h"


namespace SimpleDB {


#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {

     friend class BPlusTreeIterator<KeyType, ValueType, KeyComparator>;

    using DirectoryPage = BPlusTreeDirectoryPage<KeyType, int, KeyComparator>;
    using LeafPage = BPlusTreeLeafPage<KeyType, RID, KeyComparator>;
    using BucketPage = BPlusTreeBucketPage<KeyType, RID, KeyComparator>;


public:


    BPlusTree(const std::string &index_file_name, 
              int root_block_num,
              const KeyComparator &comparator,
              BufferManager *buffer_manager);

    /**
    * @brief
    * Return the value that associated with input key
    * @param key 
    * @param result 
    * @return true when key exists
    */
    bool GetValue(const KeyType &key, std::vector<ValueType> *result) const;



    /**
    * @brief insert a key-value pair into btree.
    * 
    * @param key
    * @param value
    */
    void Insert(const KeyType &key, const ValueType &value);
    


    /**
    * @brief remove a key-value pair in btree
    * 
    * @param key
    * @param value
    * @return true if success, false if this pair is not exist.
    */
    bool Remove(const KeyType &key, const ValueType &value);


    std::unique_ptr<BPLUSTREE_ITERATOR_TYPE> Begin();

    std::unique_ptr<BPLUSTREE_ITERATOR_TYPE> Begin(const KeyType &key);


private:


    /**
    * @brief create a new tree which only has a leaf page 
    */
    void StartNewTree();


    /**
    * @brief search for a leaf page which contains the specified key maybe.
    * 
    * @param search_key
    * @return the block num of leaf which constains the specified key.
    */
    int SearchLeaf(const KeyType &search_key) const;

    
    void ReadFromBucketChain(int first_bucket_num, 
                             std::vector<ValueType> *result) const;

    void InsertIntoBucketChain(int first_bucket_num, const ValueType &value);

    bool RemoveInBucketChain(int &first_bucket_num, 
                             const ValueType &value, 
                             bool *need_to_delete_chain);


    template<typename N>
    N* CreateBTreePage(PageType page_type_, int *new_block_num);


    void DeleteBTreePage(BPlusTreePage* old_page);


    template<class N>
    N* Split(N* old_page);





    bool BorrowLeafKey(bool from_right, int sibling_block_num, LeafPage *borrower);
    bool BorrowDirKey(bool from_right, int sibling_block_num, DirectoryPage *dir_page);


    void InsertIntoParent(int dir_block_num, const KeyType &key,
                          int left_block_num, int right_block_num);

    void UpdateChildKeyInParent(int dir_block_num, 
                                const KeyType &old_key,
                                const KeyType &new_key);

    void RemoveFromParent(int parent_block_num, const KeyType &be_removed_key);


    void UpdateRootBlockNum(int new_block_num);


    void ResetDirChildParent(DirectoryPage *dir_page);

    void ResetDirChildParentOne(DirectoryPage *dir_page, int child_block_num);



    void MergeLeafs(LeafPage *curr_leaf, int sibling_block_num, bool with_right, KeyType *be_removed_key);
    void MergeKeys(DirectoryPage *curr_dir, int sibling_block_num, bool with_right, KeyType *be_removed_key);


    


public:
    void PrintDir(int block_num) const;

    void PrintTree() const;


private:


    std::string index_file_name_;

    // root directory block num 
    int root_block_num_{0};

    // the last deleted block num
    int last_deleted_num_{INVALID_BLOCK_NUM};

    KeyComparator comparator_;

    // bufferpool
    BufferManager *buffer_manager_;
    
    // root latch to concurrency
    mutable ReaderWriterLatch root_latch_;

    // two variable in memory to debug
    // this is not necessary
    int max_dir_size_;

    int max_leaf_size_;

};

} // namespace SimpleDB


#endif