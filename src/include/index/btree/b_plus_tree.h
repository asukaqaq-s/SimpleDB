#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include "record/schema.h"
#include "index/btree/b_plus_tree_bucket_page.h"
#include "index/btree/b_plus_tree_directory_page.h"
#include "index/btree/b_plus_tree_leaf_page.h"
#include "index/btree/b_plus_tree_iterator.h"

#include <queue>


namespace SimpleDB {


#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>


class BPlusTreeContext {

public:

    BPlusTreeContext(BufferManager *buf) : buffer_manager_(buf) {}


    void AddToWritePageSet(Buffer *buffer) {
        if (std::find(write_page_set_.begin(), write_page_set_.end(), buffer) !=
            write_page_set_.end()) {

            return;
        }

        int block_num = buffer->GetBlockID().BlockNum();
        page_map_[block_num] = buffer;
        buffer->WLock();
        write_page_set_.push_front(buffer);
    }

    void AddToReadPageSet(Buffer *buffer) {
        if (std::find(read_page_set_.begin(), read_page_set_.end(), buffer) !=
            read_page_set_.end()) {
            return;
        }

        int block_num = buffer->GetBlockID().BlockNum();
        page_map_[block_num] = buffer;
        buffer->RLock();
        read_page_set_.push_front(buffer);
    }

    
    const std::deque<Buffer*>& GetWritePageSet() {
        return write_page_set_;
    }

    const std::deque<Buffer*>& GetReadPageSet() {
        return read_page_set_;
    }


    void ClearPageSet() {
        for (auto &t : write_page_set_) {
            auto block_id = t->GetBlockID();
            t->WUnlock();
            assert(buffer_manager_->UnpinBlock(block_id, true));
        }

        for (auto &t : read_page_set_) {
            auto block_id = t->GetBlockID();
            t->RUnlock();
            assert(buffer_manager_->UnpinBlock(block_id, false));
        }

        write_page_set_.clear();
        read_page_set_.clear();
    }


    Buffer *GetPageInSet(int block_num) {
        if (page_map_.find(block_num) == page_map_.end()) {
            return nullptr;
        }

        return page_map_[block_num];
    }


private:

    std::deque<Buffer *> write_page_set_;
    std::deque<Buffer *> read_page_set_;
    
    std::unordered_map<int, Buffer*> page_map_;

    BufferManager *buffer_manager_;

};


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


    std::tuple<int, int> GetBrotherNodeBlockNum(int parent_block_num, 
                                                int child_block_num, 
                                                BPlusTreeContext *context);





    bool BorrowLeafKey(bool from_right, int sibling_block_num, LeafPage *borrower, BPlusTreeContext *context);
    bool BorrowDirKey(bool from_right, int sibling_block_num, DirectoryPage *dir_page, BPlusTreeContext *context);


    void InsertIntoParent(int dir_block_num, const KeyType &key,
                          int left_block_num, int right_block_num, BPlusTreeContext *context);

    void UpdateChildKeyInParent(int dir_block_num, 
                                const KeyType &old_key,
                                const KeyType &new_key, BPlusTreeContext *context);

    void RemoveFromParent(int parent_block_num, const KeyType &be_removed_key, BPlusTreeContext *context);


    void UpdateRootBlockNum(int new_block_num);


    void ResetDirChildParent(DirectoryPage *dir_page);

    void ResetDirChildParentOne(DirectoryPage *dir_page, int child_block_num);



    void MergeLeafs(LeafPage *curr_leaf, int sibling_block_num, bool with_right, KeyType *be_removed_key, BPlusTreeContext *context);
    void MergeKeys(DirectoryPage *curr_dir, int sibling_block_num, bool with_right, KeyType *be_removed_key, BPlusTreeContext *context);


    


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