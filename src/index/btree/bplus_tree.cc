#ifndef BPLUS_TREE_CC
#define BPLUS_TREE_CC

#include "index/btree/b_plus_tree.h"

#include <mutex>

namespace SimpleDB {


// ----------------------------------------------
// |        CreateNew Tree Functions            |
// ----------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &index_file_name, 
                          int root_block_num,
                          const KeyComparator &comparator,
                          BufferManager *buffer_manager) 
                        : index_file_name_(index_file_name), 
                          root_block_num_(root_block_num),
                          comparator_(comparator),
                          buffer_manager_(buffer_manager) {WriterGuard lock(root_latch_);
    max_leaf_size_ = LeafPage::LEAF_PAGE_MAX_SIZE;
    max_dir_size_ = DirectoryPage::DIRECTORY_PAGE_MAX_SIZE;
    
    // max_leaf_size_ = 7;
    // max_dir_size_ = 7;

    if (root_block_num_ == INVALID_BLOCK_NUM) {
        StartNewTree();
    }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree() {
    int new_block_num;
    Buffer *new_page = buffer_manager_->NewBlock(index_file_name_, &new_block_num);

    // create a new leaf page to act as root
    auto *leafPage = reinterpret_cast<LeafPage*>(new_page->contents()->GetRawDataPtr());
    leafPage->Init(new_block_num, max_leaf_size_);
    UpdateRootBlockNum(new_block_num);

    // unpin to ensure persistent storage
    buffer_manager_->UnpinBlock({index_file_name_, new_block_num}, true);
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootBlockNum(int new_block_num) {
    root_block_num_ = new_block_num;
}



// ----------------------------------------------
// |           Read Data Functions              |
// ----------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) const {
    ReaderGuard lock(root_latch_);
    // create the leaf page
    int leaf_block_num = SearchLeaf(key);
    Buffer *buffer = buffer_manager_->PinBlock({index_file_name_, leaf_block_num});
    auto *leaf_page = reinterpret_cast<LeafPage*>(buffer->contents()->GetRawDataPtr());
    buffer->RLock();

    // try to access this leaf page
    ValueType tmp_value;
    bool res = leaf_page->Lookup(key, &tmp_value, comparator_);


    // check if read is sucessfully
    if (!res) {
        buffer->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, leaf_block_num}, false);
        return res;
    }
    
    
    // try to access bucket chain
    if (tmp_value.GetSlot() == -1) {
        int bucket_block_num = tmp_value.GetBlockNum();
        ReadFromBucketChain(bucket_block_num, result);
    }
    else {
        // only one record
        result->emplace_back(tmp_value);
    }


    buffer->RUnlock();
    buffer_manager_->UnpinBlock({index_file_name_, leaf_block_num}, false);
    return true;    
}



INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::SearchLeaf(const KeyType &search_key) const {
    Buffer *curr_buffer = buffer_manager_->PinBlock({index_file_name_, root_block_num_});
    BPlusTreePage *curr_page = reinterpret_cast<BPlusTreePage*>(curr_buffer->contents()->GetRawDataPtr());  
    

    // search for a leaf page 
    while (!curr_page->IsLeafPage()) {
        
        // binary search finds the child block which contains key
        auto *dir_page = reinterpret_cast<DirectoryPage*>(curr_page);
        int child_block_num = dir_page->Lookup(search_key, comparator_);


        // unpin old curr_page
        buffer_manager_->UnpinBlock({index_file_name_, dir_page->GetBlockNum()}, false);
        
        
        // create new curr_page
        curr_buffer = buffer_manager_->PinBlock({index_file_name_, child_block_num});
        curr_page = reinterpret_cast<BPlusTreePage*>(curr_buffer->contents()->GetRawDataPtr());
    }

    // unpin curr_page and return its blocknum
    int block_num = curr_page->GetBlockNum();
    buffer_manager_->UnpinBlock({index_file_name_, block_num}, false);
    return block_num;
}





// ------------------------------------------------
// |           Insert Data Functions              |
// ------------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) {WriterGuard lock(root_latch_);
    // Insert can be divided into two cases
    // 1. a key is exist so that we need to insert it into bucket chain
    // 2. a key is non-exist so that we need to insert it into leaf page

    // create bplustree execution context to ensure concurrency
    BPlusTreeContext context(buffer_manager_);

    // create the leaf page
    int leaf_block_num = SearchLeaf(key);
    Buffer *buffer = buffer_manager_->PinBlock({index_file_name_, leaf_block_num});
    auto *leaf_page = reinterpret_cast<LeafPage*>(buffer->contents()->GetRawDataPtr());
    context.AddToWritePageSet(buffer);

    
    // try to read data from leaf page and check if has this value
    bool is_success = leaf_page->Insert(key, value, comparator_);


    // 1. handle the case of duplicated keys
    if (!is_success) {

        // check if there is existing a bucket chain
        ValueType tmp_value;
        bool is_exist = leaf_page->Lookup(key, &tmp_value, comparator_);   

        if (!is_exist) {
            SIMPLEDB_ASSERT(false, "unexpectable error");
        }


        // there is a rid, create a bucket chain 
        if (tmp_value.GetSlot() != -1) {
            int new_block_num;
            BucketPage* bucket = CreateBTreePage<BucketPage>
                                 (PageType::BPLUS_TREE_BUCKET_PAGE, &new_block_num);
            if (bucket->GetBlockNum() != new_block_num) {
                assert(false);
            }

            // insert into it
            assert(bucket->Insert(tmp_value));
            assert(bucket->Insert(value)); 


            // modify value at piar_index
            int key_index = leaf_page->KeyIndexGreaterEqual(key, comparator_);
            leaf_page->SetValueAt(key_index, RID(new_block_num, -1));
            
            
            // release resource
            buffer_manager_->UnpinBlock({index_file_name_, new_block_num}, true);
        }

        // there is a bucket num, try to insert into it.
        else {
            int first_bucket_num = tmp_value.GetBlockNum();
            InsertIntoBucketChain(first_bucket_num, value);
        }


        // release w-latch and buffer
        context.ClearPageSet();
        return;
    }

   

    // 2. handle the case of normal-inserting
    //    (1) the page is not full, do nothing 
    //    (2) the page is full, split and insert an entry into parent
    if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
        // Split and maintain a leaf linked list 
        LeafPage *new_leaf_page= Split<LeafPage>(leaf_page);
        int new_leaf_block_num = new_leaf_page->GetBlockNum();
        new_leaf_page->SetNextBlockNum(leaf_page->GetNextBlockNum());
        leaf_page->SetNextBlockNum(new_leaf_block_num);


        // get the middle key and will push up this key
        KeyType middle_key = new_leaf_page->KeyAt(0);
        int leaf_page_parent_block_num = leaf_page->GetParentBlockNum();


        // release new leaf, should we need to acquire w-lock of sibling node?
        assert(buffer_manager_->UnpinBlock({index_file_name_, new_leaf_block_num}, true));
        

        // insert into parent directory
        InsertIntoParent(leaf_page_parent_block_num, middle_key,
                         leaf_block_num, new_leaf_block_num, &context);
    }
    

    // release w-latch and buffer
    context.ClearPageSet();
    
    
}


INDEX_TEMPLATE_ARGUMENTS
template<class N>
N* BPLUSTREE_TYPE::Split(N* old_page) {
    int new_block_num;

    // handle leaf page
    if (old_page->GetPageType() == PageType::BPLUS_TREE_LEAF_PAGE) {
        LeafPage *new_leaf_page = CreateBTreePage<LeafPage>
                                  (PageType::BPLUS_TREE_LEAF_PAGE, &new_block_num);
        new_leaf_page->SetParentBlockNum(old_page->GetParentBlockNum());
    
        reinterpret_cast<LeafPage *>(old_page)->MoveHalfTo(new_leaf_page);
        return reinterpret_cast<N*>(new_leaf_page);
    }


    DirectoryPage *new_dir_page = CreateBTreePage<DirectoryPage>
                                  (PageType::BPLUS_TREE_DIRECTORY_PAGE, &new_block_num);
    new_dir_page->SetParentBlockNum(old_page->GetParentBlockNum());
    
    reinterpret_cast<DirectoryPage*>(old_page)->MoveHalfTo(new_dir_page);
    return reinterpret_cast<N*>(new_dir_page);
}






INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(int dir_block_num, const KeyType &key,
                                      int left_block_num, int right_block_num, BPlusTreeContext *context) {
    
    // if left child is the root_page, we should create a new root_page to replace it 
    if (dir_block_num == INVALID_BLOCK_NUM) {
        int new_root_block_num;
        DirectoryPage *parent_dir_page = CreateBTreePage<DirectoryPage>
                                         (PageType::BPLUS_TREE_DIRECTORY_PAGE, &new_root_block_num);

        // init new root of btree
        UpdateRootBlockNum(new_root_block_num);
        parent_dir_page->PopulateNewRoot(left_block_num, key, right_block_num);
    

        // redistributed parent block of child
        ResetDirChildParent(parent_dir_page);
        buffer_manager_->UnpinBlock({index_file_name_, new_root_block_num}, true);
        return;
    }
    

    // get parent block
    Buffer *dir_buffer = buffer_manager_->PinBlock({index_file_name_, dir_block_num});
    DirectoryPage *dir_page = reinterpret_cast<DirectoryPage*>(dir_buffer->contents()->GetRawDataPtr());
    context->AddToWritePageSet(dir_buffer);

    
    // insert into directory page
    assert(dir_page->GetSize() <= dir_page->GetMaxSize());
    dir_page->InsertNodeAfter(left_block_num, key, right_block_num);

    
    // since directory_page tolerates size not splitting immediately when 
    // it reaches maxsize, it needs to be checked before insertion
    if (dir_page->GetSize() > dir_page->GetMaxSize()) { 
        assert(dir_page->GetSize() == dir_page->GetMaxSize() + 1);
        DirectoryPage *sibling_dir_page = Split<DirectoryPage>(dir_page);
        int sibling_dir_block_num = sibling_dir_page->GetBlockNum();
        KeyType middle_key = sibling_dir_page->KeyAt(0);
        

        InsertIntoParent(dir_page->GetParentBlockNum(), middle_key, 
                         dir_block_num, sibling_dir_block_num, context);
        ResetDirChildParent(sibling_dir_page);


        // only unpin sibling buffer
        buffer_manager_->UnpinBlock({index_file_name_, sibling_dir_block_num}, true);
    }
    
    // don't need to split and we will unpin dir page in insert function.
}


// ---------------------------------------------------
// |             Delete Data functions               |
// ---------------------------------------------------

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Remove(const KeyType &key, const ValueType &value) {WriterGuard lock(root_latch_);
    // remove has two cases:
    //     1. this key appears multiple time, only search bucket and remove this pair
    //     2. this key appears only once, check if need to borrow key or merge
    BPlusTreeContext context(buffer_manager_);
    
    // create the leaf page
    int leaf_block_num = SearchLeaf(key);
    Buffer *buffer = buffer_manager_->PinBlock({index_file_name_, leaf_block_num});
    auto *leaf_page = reinterpret_cast<LeafPage*>(buffer->contents()->GetRawDataPtr());
    context.AddToWritePageSet(buffer);
    

    // try to remove it
    bool key_is_first = comparator_(leaf_page->KeyAt(0), key) == 0;
    bool remove_is_successful = leaf_page->Remove(key, value, comparator_); 
    

    // if fail, check if there is a bucket chain
    if (!remove_is_successful) {
        ValueType tmp_value;
        bool is_exist = leaf_page->Lookup(key, &tmp_value, comparator_);


        // this pair is not exist in tree
        if (!is_exist) {
            context.ClearPageSet();
            return false;    
        }


        assert(tmp_value.GetSlot() == -1);
        int first_bucket_num = tmp_value.GetBlockNum();
        bool need_to_delete_chain = false;
        bool is_exist_in_bucket = RemoveInBucketChain(first_bucket_num, value, &need_to_delete_chain);
        assert(first_bucket_num != INVALID_BLOCK_NUM);


        // maybe we need to update the corresponding value in leaf page
        int key_index = leaf_page->KeyIndexGreaterEqual(key, comparator_);
        if (comparator_(leaf_page->KeyAt(key_index), key) != 0) {
            assert(false);
        }


        if (need_to_delete_chain) {
            std::vector<ValueType> tmp_array;
            ReadFromBucketChain(first_bucket_num, &tmp_array);
            leaf_page->SetValueAt(key_index, tmp_array[0]);
        }
        else {
            leaf_page->SetValueAt(key_index, RID(first_bucket_num, -1));
        }


        // release resource
        context.ClearPageSet();
        return is_exist_in_bucket;
    }


    
    // remove is successful, check if need to merge or borrow a key
    int min_num = leaf_block_num == root_block_num_ ? 0 : leaf_page->GetMinSize();
    int leaf_page_size = leaf_page->GetSize();
    assert(leaf_page_size >= 0);


    // update the key in parent block
    if (key_is_first && (root_block_num_ != leaf_block_num)) {
        UpdateChildKeyInParent(leaf_page->GetParentBlockNum(), key, 
                               leaf_page->KeyAt(0), &context);
    } 
    


    if (leaf_page_size < min_num) {

        // read parent_block_num before merging to avoid this field be modified
        bool have_borrowed = false;
        int parent_block_num = leaf_page->GetParentBlockNum();
        
        // get the sibling page block num
        auto [left_page_block_num, right_page_block_num] = 
             GetBrotherNodeBlockNum(parent_block_num, leaf_block_num, &context);


        // avoid error
        if (left_page_block_num == -1 && right_page_block_num == -1) {
            SIMPLEDB_ASSERT(false, "the leaf page should has sibling node");
        }


        // try to borrow a key from the left sibling node
        if (left_page_block_num != -1) {
            have_borrowed = BorrowLeafKey(false, left_page_block_num, leaf_page, &context);
        }
        
        // try to borrow a key from the right sibling node
        if (!have_borrowed && right_page_block_num != -1) {
            have_borrowed = BorrowLeafKey(true, right_page_block_num, leaf_page, &context);
        }

        
        // finally, try to merge, merge rule:
        //    1. if it is not the last node, merge with the right brother node
        //    2. if it is the last node, merge with the left brother node
        if (!have_borrowed) {

            // the removed_key will help us to update the middle key
            KeyType be_removed_key;
            

            // not the last node
            // merge | curr | right |
            if (right_page_block_num != -1) { 
                MergeLeafs(leaf_page, right_page_block_num, true, &be_removed_key, &context);
            }
            // the last node
            // merge | left | right |
            else {
                MergeLeafs(leaf_page, left_page_block_num, false, &be_removed_key, &context);
            }
            
            
            RemoveFromParent(parent_block_num, be_removed_key, &context);

            // do update in parent block if this key is the first key or middle key
            if (key_is_first && (root_block_num_ != leaf_block_num)) {
                UpdateChildKeyInParent(leaf_page->GetParentBlockNum(), key, leaf_page->KeyAt(0), &context);
            } 

        }
    
    }



    context.ClearPageSet();
    return true;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromParent(int block_num, 
                                      const KeyType &be_removed_key, BPlusTreeContext *context) {
    // get parent_dir_page
    Buffer *dir_buffer = context->GetPageInSet(block_num);
    if (dir_buffer == nullptr) {
        dir_buffer = buffer_manager_->PinBlock({index_file_name_, block_num});
        context->AddToWritePageSet(dir_buffer);
    }
    
    DirectoryPage *dir_page = reinterpret_cast<DirectoryPage*>
                              (dir_buffer->contents()->GetRawDataPtr());


    // remove this key
    int key_index = dir_page->KeyIndex(be_removed_key, comparator_);
    bool key_is_first = (key_index == 0);
    int old_first_child_num = dir_page->ValueAt(0);
    

    if (comparator_(dir_page->KeyAt(key_index), be_removed_key) == 0) {
        dir_page->RemoveAt(key_index);
    }
   

    // get the min_size and current size
    int min_num = (block_num == root_block_num_) ? 2 : dir_page->GetMinSize();
    int curr_size = dir_page->GetSize();
    

    // check if this page is root and its size is one, update to new root
    if (block_num == root_block_num_ && curr_size < min_num) {
        root_block_num_ = old_first_child_num;
        DeleteBTreePage(dir_page);
        return;
    }


    // we need to update the key of child in the parent dir before merge and borrow.
    if (key_is_first && block_num != root_block_num_) {
        UpdateChildKeyInParent(dir_page->GetParentBlockNum(), be_removed_key, 
                        dir_page->KeyAt(dir_page->GetSize() - 1), context);
    }

     

    // if this page is not root, borrow key and merge like leaf page
    // don't forget that update if this key is the middle key
    if (curr_size < min_num) {
        bool have_borrowed = false;
        int parent_block_num = dir_page->GetBlockNum();
        
        // get the sibling page block num
        auto [left_page_block_num, right_page_block_num] = 
             GetBrotherNodeBlockNum(parent_block_num, block_num, context);
        

        // avoid error
        if (left_page_block_num == -1 && right_page_block_num == -1) {
            SIMPLEDB_ASSERT(false, "the leaf page should has sibling node");
        }
        
        // try to borrow keys from left sibling node
        if (left_page_block_num != -1) {
            have_borrowed = BorrowDirKey(false, left_page_block_num, dir_page, context);
        }

        // try to borrow keys from right sibling node
        if (!have_borrowed && right_page_block_num != -1) {
            have_borrowed = BorrowDirKey(true, right_page_block_num, dir_page, context);
        }


        // merge with sibling node, remember that reset children's parent
        if (!have_borrowed) {
            
            KeyType be_removed_key_in_parent;
            int parent_block_num = dir_page->GetParentBlockNum();

            // if this dir_page is the last page of parent
            // merge  | prev | dir_page |
            if (right_page_block_num == -1) {
                MergeKeys(dir_page, left_page_block_num, false, &be_removed_key_in_parent, context);
            }
            // else merge | dir_page | next |
            else {
                MergeKeys(dir_page, right_page_block_num, true, &be_removed_key_in_parent, context);
            }
            

            
            // if merge is successful, we need update the information of parent 
            RemoveFromParent(parent_block_num, be_removed_key_in_parent, context);
            if (key_is_first && block_num != root_block_num_) {
                UpdateChildKeyInParent(dir_page->GetParentBlockNum(), be_removed_key_in_parent, 
                        dir_page->KeyAt(dir_page->GetSize() - 1), context);
            }
        }
    }

}




// -----------------------------------------------------------
// |             Borrow key relative functions               |
// -----------------------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::BorrowLeafKey(bool from_right, 
                                   int sibling_block_num, 
                                   LeafPage *borrower, 
                                   BPlusTreeContext *context) {

    // get sibling_leaf_page
    Buffer *sibling_buffer = buffer_manager_->PinBlock({index_file_name_, sibling_block_num});
    LeafPage *lender = reinterpret_cast<LeafPage*>
                            (sibling_buffer->contents()->GetRawDataPtr());
    context->AddToWritePageSet(sibling_buffer);


    // check if we can borrow key from this page
    int min_size = lender->GetMinSize();
    int lender_size = lender->GetSize();

    
    SIMPLEDB_ASSERT(lender_size >= min_size, "this page should merge before borrowing key");    
    if (lender_size > min_size) {
        // note that leaf page don't need to reset children's parent in borrow key
        
        // get parent_block_num
        int parent_block_num = borrower->GetParentBlockNum();


        // if we borrow a key from right sibling page, we should do the following things
        // 1. move the first key of lender to borrower
        // 2. update the key of lender in parent_dir_page
        if (from_right) {
            KeyType old_key = lender->KeyAt(0);
            KeyType new_key = lender->KeyAt(1);

            lender->MoveFirstToEndOf(borrower);
            UpdateChildKeyInParent(parent_block_num, old_key, new_key, context);
        }


        // if we borrow a key from left sibling page, we should do the following things
        // 1. move the last key of lender to borrower
        // 2. update the key of borrower in parent_dir_page
        if (!from_right) {
            KeyType old_key = borrower->KeyAt(0);
            KeyType new_key = lender->KeyAt(lender->GetSize() - 1);

            lender->MoveLastToFrontOf(borrower);
            UpdateChildKeyInParent(parent_block_num, old_key, new_key, context);
        }

        return true;
    }

    return false;
}


INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::BorrowDirKey(bool from_right, 
                                  int sibling_block_num, 
                                  DirectoryPage *borrower, 
                                  BPlusTreeContext *context) {
    
    // get sibling_dir_page
    Buffer *sibling_buffer = buffer_manager_->PinBlock({index_file_name_, sibling_block_num});
    DirectoryPage *lender = reinterpret_cast<DirectoryPage*>
                            (sibling_buffer->contents()->GetRawDataPtr());
    context->AddToWritePageSet(sibling_buffer);


    // check if we can borrow key from this page
    int min_size = lender->GetMinSize();
    int lender_size = lender->GetSize();


    SIMPLEDB_ASSERT(lender_size >= min_size, "this page should merge before borrowing key");    
    if (lender_size > min_size) {
        // note that directory page need to reset children's parent in borrow key
        
        // get parent_block_num
        int parent_block_num = borrower->GetParentBlockNum();
        assert(parent_block_num != INVALID_BLOCK_NUM);


        // if we borrow a key from right sibling page, we should do the following things
        // 1. move the first key of lender to borrower
        // 2. update the key of borrow in parent_dir_page
        if (from_right) {
            KeyType old_key = lender->KeyAt(0);
            KeyType new_key = lender->KeyAt(1);
            int new_child_block_num = lender->ValueAt(0);

            lender->MoveFirstToEndOf(borrower);
            UpdateChildKeyInParent(parent_block_num, old_key, new_key, context);
            ResetDirChildParentOne(borrower, new_child_block_num);
        }


        // if we borrow a key from left sibling page, we should do the following things
        // 1. move the last key of lender to borrower
        // 2. update the key of borrower in parent_dir_page
        if (!from_right) {
            int lender_curr_size = lender->GetSize();
            KeyType old_key = borrower->KeyAt(0);
            KeyType new_key = lender->KeyAt(lender_curr_size - 1);
            int new_child_block_num = lender->ValueAt(lender_curr_size - 1);

            lender->MoveLastToFrontOf(borrower);
            UpdateChildKeyInParent(parent_block_num, old_key, new_key, context);
            ResetDirChildParentOne(borrower, new_child_block_num);
        }

        return true;
    }


    return false;
}



// ---------------------------------------------------
// |            Merge relative functions             |
// ---------------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeafs(LeafPage *curr_leaf, 
                                int sibling_block_num, 
                                bool with_right, 
                                KeyType *be_removed_key, 
                                BPlusTreeContext *context) {
    
    // Get the sibling node
    Buffer *sibling_buffer = context->GetPageInSet(sibling_block_num);
    if (sibling_buffer == nullptr) {
        sibling_buffer = buffer_manager_->PinBlock({index_file_name_, sibling_block_num});
        context->AddToWritePageSet(sibling_buffer);
    }


    LeafPage *sibling_leaf = reinterpret_cast<LeafPage*>
                             (sibling_buffer->contents()->GetRawDataPtr());


    if (with_right) { 
        // delete sibling leaf
        *be_removed_key = sibling_leaf->KeyAt(0);
        sibling_leaf->MoveAllTo(curr_leaf);
        DeleteBTreePage(sibling_leaf);
    }
    else {
        // delete curr leaf   
        *be_removed_key = curr_leaf->KeyAt(0);
        curr_leaf->MoveAllTo(sibling_leaf);
        DeleteBTreePage(curr_leaf);
    }
    

}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeKeys(DirectoryPage *curr_dir, 
                               int sibling_block_num, 
                               bool with_right, 
                               KeyType *be_removed_key, 
                               BPlusTreeContext *context) {
    
    // Get the sibling node
    Buffer *sibling_buffer = context->GetPageInSet(sibling_block_num);
    if (sibling_buffer == nullptr) {
        sibling_buffer = buffer_manager_->PinBlock({index_file_name_, sibling_block_num});
        context->AddToWritePageSet(sibling_buffer);
    }


    DirectoryPage *sibling_dir = reinterpret_cast<DirectoryPage*>
                                 (sibling_buffer->contents()->GetRawDataPtr());


    if (with_right) { 
        // delete sibling  
        *be_removed_key = sibling_dir->KeyAt(0);
        sibling_dir->MoveAllTo(curr_dir);
        ResetDirChildParent(curr_dir);
        DeleteBTreePage(sibling_dir);        
    }
    else {
        // delete curr leaf   
        *be_removed_key = curr_dir->KeyAt(0);
        curr_dir->MoveAllTo(sibling_dir);
        ResetDirChildParent(sibling_dir);
        DeleteBTreePage(curr_dir);
    }
    

}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateChildKeyInParent(int parent_block_num, 
                                            const KeyType &old_key,
                                            const KeyType &new_key, 
                                            BPlusTreeContext *context) {
    assert(parent_block_num != INVALID_BLOCK_NUM);

    // get a parent buffer, if this page have existed in the context of this thread,
    // we don't need to acquire buffer again. 
    Buffer *parent_buffer = context->GetPageInSet(parent_block_num);
    if (parent_buffer == nullptr) {
        parent_buffer = buffer_manager_->PinBlock({index_file_name_, parent_block_num});
        context->AddToWritePageSet(parent_buffer);
    }
    
    
    // create a parent page according to parent buffer.
    DirectoryPage *parent_page = reinterpret_cast<DirectoryPage*>
                  (parent_buffer->contents()->GetRawDataPtr());
    context->AddToWritePageSet(parent_buffer);


    // get the index of old_key
    int key_index = parent_page->KeyIndex(old_key, comparator_);
    int ancestor_block_num = parent_page->GetParentBlockNum();
    
    
    // key of the first child of parent is null
    if (comparator_(parent_page->KeyAt(key_index), old_key) != 0) {
        return;
    }
    

    // update to new_key and write to disk
    parent_page->SetKeyAt(key_index, new_key);


    // if old_key is the middle key, we also need to update it in ancestor node
    if (key_index == 0 && ancestor_block_num != INVALID_BLOCK_NUM) {
        UpdateChildKeyInParent(ancestor_block_num, old_key, new_key, context);
    }
}


INDEX_TEMPLATE_ARGUMENTS
std::tuple<int, int> BPLUSTREE_TYPE::GetBrotherNodeBlockNum(int parent_block_num, 
                                                            int child_block_num, 
                                                            BPlusTreeContext *context) {
    // check if we have acquired this buffer before.
    Buffer *parent_buffer = context->GetPageInSet(parent_block_num);
    if (parent_buffer == nullptr) {
        parent_buffer = buffer_manager_->PinBlock({index_file_name_, parent_block_num});
        context->AddToWritePageSet(parent_buffer);
    }

    // create a parent directory page
    DirectoryPage *parent_page = reinterpret_cast<DirectoryPage*>
                                (parent_buffer->contents()->GetRawDataPtr());
    int left_page_block_num = -1, right_page_block_num = -1;

    // update variable
    int index = parent_page->ValueIndex(child_block_num);
    bool is_the_first_child = (index == 0);
    bool is_the_last_child = (index == parent_page->GetSize() - 1);


    // get page block num
    if (!is_the_first_child) {
        left_page_block_num = parent_page->ValueAt(index - 1);
    }
    if (!is_the_last_child) {
        right_page_block_num = parent_page->ValueAt(index + 1);
    }
            
    return std::make_tuple(left_page_block_num, right_page_block_num);
}



// ----------------------------------------------------
// |             Read/Write data in Bucket            |
// ----------------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReadFromBucketChain(int first_bucket_num, 
                                         std::vector<ValueType> *result) const {
    int next_bucket_num = first_bucket_num;

    while (next_bucket_num != INVALID_BLOCK_NUM) {
        
        // create bucket
        auto *curr_page = buffer_manager_->PinBlock({index_file_name_, next_bucket_num});
        auto *bucket = reinterpret_cast<BucketPage*>(curr_page->contents()->GetRawDataPtr());
        assert(bucket->GetPageType() == PageType::BPLUS_TREE_BUCKET_PAGE);

        
        // read data
        bucket->GetValue(result);
        int old_bucket_num = next_bucket_num;
        next_bucket_num = bucket->GetNextBucketNum();
        

        // release resource
        buffer_manager_->UnpinBlock({index_file_name_, old_bucket_num}, false);
    }
    
}





INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoBucketChain(int first_bucket_num, const ValueType &value) {
    int old_bucket_num = first_bucket_num;
    int next_bucket_num = first_bucket_num;
    bool insert_is_successful = false;


    while (next_bucket_num != INVALID_BLOCK_NUM) {
        
        // create bucket
        auto *curr_page = buffer_manager_->PinBlock({index_file_name_, next_bucket_num});
        auto *bucket = reinterpret_cast<BucketPage*>(curr_page->contents()->GetRawDataPtr());
        assert(bucket->GetPageType() == PageType::BPLUS_TREE_BUCKET_PAGE);


        // try insert into it
        insert_is_successful |= bucket->Insert(value);
        old_bucket_num = next_bucket_num;
        next_bucket_num = bucket->GetNextBucketNum();


        // release resource
        buffer_manager_->UnpinBlock({index_file_name_, old_bucket_num}, insert_is_successful);

        if (insert_is_successful) {
            break;
        }
    }


    // can't insert, create a new bucket
    if (!insert_is_successful) {
        // create new bucket page
        int free_block_num;
        BucketPage *bucket = CreateBTreePage<BucketPage>
                            (PageType::BPLUS_TREE_BUCKET_PAGE, &free_block_num);


        // try to insert into it
        insert_is_successful = bucket->Insert(value);
        assert(insert_is_successful);


        // fetch prev bucket page
        auto *prev_page = buffer_manager_->PinBlock({index_file_name_, old_bucket_num});
        auto *prev_bucket = reinterpret_cast<BucketPage*>(prev_page->contents()->GetRawDataPtr()); 
        prev_bucket->SetNextBucketNum(free_block_num);


        // release resource
        buffer_manager_->UnpinBlock({index_file_name_, free_block_num}, true);
        buffer_manager_->UnpinBlock({index_file_name_, old_bucket_num}, true);   
    }
}



INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::RemoveInBucketChain(int &first_bucket_num, 
                                         const ValueType &value, 
                                         bool *need_to_delete_chain) {
    
    // count for checking if need to delete the bucket chain
    int record_count = 0;
    int prev_block_num = INVALID_BLOCK_NUM;
    int curr_bucket_num = first_bucket_num;
    bool remove_is_successful = false;
    assert(first_bucket_num != INVALID_BLOCK_NUM);
    
    std::function<void(int, BucketPage*)> prev_to_next = [&]
            (int prev_block_num, BucketPage *curr_bucket) {
        if (prev_block_num != INVALID_BLOCK_NUM) {
            auto *prev_buffer = buffer_manager_->PinBlock({index_file_name_, prev_block_num});
            BucketPage *prev_bucket = reinterpret_cast<BucketPage*>
                                      (prev_buffer->contents()->GetRawDataPtr());
            prev_bucket->SetNextBucketNum(curr_bucket->GetNextBucketNum());
            buffer_manager_->UnpinBlock({index_file_name_, prev_block_num}, true);
        }
    };


    while (curr_bucket_num != INVALID_BLOCK_NUM) {
        auto *curr_buffer = buffer_manager_->PinBlock({index_file_name_, curr_bucket_num});
        BucketPage *curr_bucket = reinterpret_cast<BucketPage*>
                                  (curr_buffer->contents()->GetRawDataPtr());
        
        // try to remove it
        remove_is_successful |= curr_bucket->Remove(value);
        int curr_bucket_size = curr_bucket->GetSize();
        record_count += curr_bucket_size;


        // check if need to delete this bucket
        if (curr_bucket_size == 0) {   
            prev_to_next(prev_block_num, curr_bucket);            
            DeleteBTreePage(curr_bucket);  

            // update header node of linklist
            if (curr_bucket_num == first_bucket_num) {
                first_bucket_num = curr_bucket->GetNextBucketNum();
            }  
        }

        
        // release resource
        bool is_dirty = remove_is_successful | (!curr_bucket_size);
        prev_block_num = curr_bucket_num;
        curr_bucket_num = curr_bucket->GetNextBucketNum();
        buffer_manager_->UnpinBlock({index_file_name_, prev_block_num}, is_dirty);
    

        if (remove_is_successful) {
            break;
        }

    }


    // only delete the chain when finish traversing the entire bucket chain.
    if (record_count == 1 && curr_bucket_num == INVALID_BLOCK_NUM) {
        *need_to_delete_chain = true;   
    }

    return remove_is_successful;
}




// ------------------------------------------------
// |           Insert relative Functions          |
// ------------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N* BPLUSTREE_TYPE::CreateBTreePage(PageType page_type_, int *new_block_num) {
    // prefer to create a btree page from the deleted page
    Buffer *tmp_buffer = nullptr;
    BPlusTreePage* tmp_page = nullptr;

    if (last_deleted_num_ == INVALID_BLOCK_NUM) {
        tmp_buffer = buffer_manager_->NewBlock(index_file_name_, new_block_num);
        tmp_page = reinterpret_cast<BPlusTreePage*>(tmp_buffer->contents()->GetRawDataPtr());
    }
    else {
        tmp_buffer = buffer_manager_->PinBlock({index_file_name_, last_deleted_num_});
        *new_block_num = last_deleted_num_;
        tmp_page = reinterpret_cast<BPlusTreePage*>(tmp_buffer->contents()->GetRawDataPtr());
        last_deleted_num_ = tmp_page->GetDeletedBlockNum();
    }
    


    // reset infor
    switch (page_type_)
    {
    case PageType::BPLUS_TREE_LEAF_PAGE:
    {
        LeafPage *new_page = reinterpret_cast<LeafPage*>(tmp_page);
        new_page->Init(*new_block_num, max_leaf_size_);
        return reinterpret_cast<N*>(new_page);
    }


    case PageType::BPLUS_TREE_DIRECTORY_PAGE:
    {
        DirectoryPage *new_page = reinterpret_cast<DirectoryPage*>(tmp_page);
        new_page->Init(*new_block_num, max_dir_size_);
        return reinterpret_cast<N*>(new_page);
    }

    case PageType::BPLUS_TREE_BUCKET_PAGE:
    {
        BucketPage *new_page = reinterpret_cast<BucketPage*>(tmp_page);
        new_page->Init(*new_block_num);
        return reinterpret_cast<N*>(new_page);
    }
    
    default:
        assert(false);
        break;
    }


    return nullptr;
}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteBTreePage(BPlusTreePage* old_page) {
    // the flag is negative for ease of distinction
    old_page->SetPageType(PageType::DEFAULT_PAGE_TYPE);
    old_page->SetDeletedBlockNum(last_deleted_num_);
    last_deleted_num_ = old_page->GetBlockNum();
}




INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ResetDirChildParent(DirectoryPage *dir_page) {
    int size = dir_page->GetSize();
    
    for (int i = 0;i < size; i++) {
        int child_num = dir_page->ValueAt(i);
        ResetDirChildParentOne(dir_page, child_num);
    }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ResetDirChildParentOne(DirectoryPage *dir_page, 
                                            int child_block_num) {

    int parent_block_num = dir_page->GetBlockNum();
    auto *child_buffer = buffer_manager_->PinBlock({index_file_name_, child_block_num});
    auto *child_bplus_page = reinterpret_cast<BPlusTreePage*>
                             (child_buffer->contents()->GetRawDataPtr());
    PageType page_type = child_bplus_page->GetPageType();
    bool need_to_set = (child_bplus_page->GetParentBlockNum() != parent_block_num);

    
    // if we don't need to, just return immediately and don't need to flush to disk
    if (!need_to_set) {
        buffer_manager_->UnpinBlock({index_file_name_, child_block_num}, false);
        return;
    }

    
    // set parent blocknum and flush to disk
    switch (page_type)
    {
    case PageType::BPLUS_TREE_DIRECTORY_PAGE:
    {
        auto *child_dir_page = reinterpret_cast<DirectoryPage*>(child_bplus_page);
        child_dir_page->SetParentBlockNum(parent_block_num);
        break;
    }

    case PageType::BPLUS_TREE_LEAF_PAGE:
    {
        auto *child_leaf_page = reinterpret_cast<LeafPage*>(child_bplus_page);
        child_leaf_page->SetParentBlockNum(parent_block_num);
        break;
    }

    default:
        assert(false);
        break;
    }


    buffer_manager_->UnpinBlock({index_file_name_, child_block_num}, true);
}


INDEX_TEMPLATE_ARGUMENTS
std::unique_ptr<BPLUSTREE_ITERATOR_TYPE> BPLUSTREE_TYPE::Begin() {
    // move to the first leaf node
    ReaderGuard lock(root_latch_);
    Buffer *curr_buffer = buffer_manager_->PinBlock({index_file_name_, root_block_num_});
    BPlusTreePage *curr_page = reinterpret_cast<BPlusTreePage*>(curr_buffer->contents()->GetRawDataPtr());  
    curr_buffer->RLock();


    // search for a leaf page 
    while (!curr_page->IsLeafPage()) {
        
        // move to the first child
        auto *dir_page = reinterpret_cast<DirectoryPage*>(curr_page);
        int child_block_num = dir_page->ValueAt(0);


        // unpin old curr_page
        curr_buffer->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, dir_page->GetBlockNum()}, false);
        
        
        // create new curr_page
        curr_buffer = buffer_manager_->PinBlock({index_file_name_, child_block_num});
        curr_page = reinterpret_cast<BPlusTreePage*>(curr_buffer->contents()->GetRawDataPtr());
        curr_buffer->RLock();
    }



    // if exist, this page is a leaf page
    int block_num = curr_page->GetBlockNum();
    // check some special cases
    if (block_num == root_block_num_ && curr_page->GetSize() == 0) {
        block_num = INVALID_BLOCK_NUM;
        curr_buffer->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, root_block_num_}, false);
        curr_buffer = nullptr;
    }
    
    return std::make_unique<BPLUSTREE_ITERATOR_TYPE>(0, block_num, curr_buffer, this);
}


INDEX_TEMPLATE_ARGUMENTS
std::unique_ptr<BPLUSTREE_ITERATOR_TYPE> BPLUSTREE_TYPE::Begin(const KeyType &search_key) {
    // move to the first key which greater equal than search key
    ReaderGuard lock(root_latch_);
    int curr_block_num = SearchLeaf(search_key);
    Buffer *curr_buffer = buffer_manager_->PinBlock({index_file_name_, curr_block_num});
    auto *curr_leaf = reinterpret_cast<LeafPage*>(curr_buffer->contents()->GetRawDataPtr());

    curr_buffer->RLock();
    int curr_slot = curr_leaf->KeyIndexGreaterEqual(search_key, comparator_);
    

    // two special cases:
    // 1. the tree is empty
    // 2. bigger than any keys of block
    if (curr_block_num == root_block_num_ && curr_leaf->GetSize() == 0) {
        curr_block_num = INVALID_BLOCK_NUM;
    }
    else if (curr_slot == curr_leaf->GetSize() - 1 && 
             comparator_(search_key, curr_leaf->KeyAt(curr_leaf->GetSize() - 1)) > 0) {
        int next_block_num = curr_leaf->GetNextBlockNum();
        curr_block_num = next_block_num;

        if (curr_block_num != INVALID_BLOCK_NUM) {
            curr_buffer->RUnlock();
            buffer_manager_->UnpinBlock({index_file_name_, curr_block_num}, false);
        
            // create new buffer and new leaf page
            curr_buffer = buffer_manager_->PinBlock({index_file_name_, curr_block_num});
            curr_leaf = reinterpret_cast<LeafPage*>(curr_buffer->contents()->GetRawDataPtr());
            curr_slot = 0;
            curr_buffer->RLock();
        }
    }
    

    if (curr_block_num == INVALID_BLOCK_NUM) {
        curr_buffer->RUnlock();
        buffer_manager_->UnpinBlock({index_file_name_, root_block_num_}, false);
        curr_buffer = nullptr;
    }

    return std::make_unique<BPLUSTREE_ITERATOR_TYPE>(curr_slot, curr_block_num, curr_buffer, this);
}



INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintDir(int block_num) const {
    assert(block_num != INVALID_BLOCK_NUM);
    auto *buffer = buffer_manager_->PinBlock({index_file_name_, block_num});
    auto *bplus_page = reinterpret_cast<BPlusTreePage*>
                             (buffer->contents()->GetRawDataPtr());
    auto *dir_page = reinterpret_cast<DirectoryPage*>(bplus_page);

    std::cout << "start  print  dir   " << std::endl; 

    dir_page->PrintDir();
    int size = dir_page->GetSize();
    for (int i = 0; i < size; i++) {
        auto *child_buffer = buffer_manager_->PinBlock({index_file_name_, dir_page->ValueAt(i) });
        auto *child_bplus_page = reinterpret_cast<BPlusTreePage*>
                             (child_buffer->contents()->GetRawDataPtr());
        auto *child_dir_page = reinterpret_cast<LeafPage*>(child_bplus_page);

        child_dir_page->PrintLeaf();
        buffer_manager_->UnpinBlock(child_buffer);
    }

    buffer_manager_->UnpinBlock(buffer);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree() const {
    auto *buffer = buffer_manager_->PinBlock({index_file_name_, root_block_num_});
    auto *bplus_page = reinterpret_cast<BPlusTreePage*>
                             (buffer->contents()->GetRawDataPtr());

    if (bplus_page->GetPageType() == PageType::BPLUS_TREE_LEAF_PAGE) {
        auto *leaf_page = reinterpret_cast<LeafPage*>(bplus_page);
        leaf_page->PrintLeaf();
        buffer_manager_->UnpinBlock(buffer);
        return;
    }
    std::cout << "\n\n" << std::endl;
    
    auto *dir_page = reinterpret_cast<DirectoryPage*>(bplus_page);

    dir_page->PrintDir();
    int size = dir_page->GetSize();
    for (int i = 0; i < size; i++) {
        std::cout << "\n\n" << std::endl;
        auto *child_buffer = buffer_manager_->PinBlock({index_file_name_, dir_page->ValueAt(i) });
        auto *child_bplus_page = reinterpret_cast<BPlusTreePage*>
                                 (child_buffer->contents()->GetRawDataPtr());

        if (child_bplus_page->GetPageType() == PageType::BPLUS_TREE_DIRECTORY_PAGE) {
            std::cout << " root's child " << i << " is a dir " << std::endl;
            PrintDir(dir_page->ValueAt(i));
        }
        else {
            std::cout << " root's child " << i << " is a leaf " << std::endl;
            auto *child_dir_page = reinterpret_cast<LeafPage*>(child_bplus_page);
            child_dir_page->PrintLeaf();
        }

        
        buffer_manager_->UnpinBlock(child_buffer);
    }

    buffer_manager_->UnpinBlock(buffer);
}




template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace SimpleDB



#endif