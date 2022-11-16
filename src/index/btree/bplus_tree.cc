#ifndef BPLUS_TREE_CC
#define BPLUS_TREE_CC

#include "index/btree/b_plus_tree.h"


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
                          buffer_manager_(buffer_manager) {
    max_leaf_size_ = LeafPage::LEAF_PAGE_MAX_SIZE;
    max_dir_size_ = DirectoryPage::DIRECTORY_PAGE_MAX_SIZE;
    
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
    
    // create the leaf page
    int leaf_block_num = SearchLeaf(key);
    Buffer *buffer = buffer_manager_->PinBlock({index_file_name_, leaf_block_num});
    auto *leaf_page = reinterpret_cast<LeafPage*>(buffer->contents()->GetRawDataPtr());


    // try to access this leaf page
    ValueType tmp_value;
    bool res = leaf_page->Lookup(key, &tmp_value, comparator_);
    
    // check if read is sucessfully
    if (!res) {
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
void BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) {
    // Insert can be divided into two cases
    // 1. a key is exist so that we need to insert it into bucket chain
    // 2. a key is non-exist so that we need to insert it into leaf page


    // create the leaf page
    int leaf_block_num = SearchLeaf(key);
    Buffer *buffer = buffer_manager_->PinBlock({index_file_name_, leaf_block_num});
    auto *leaf_page = reinterpret_cast<LeafPage*>(buffer->contents()->GetRawDataPtr());
    
    
    // try to read data from leaf page and check if has this value
    bool is_success = leaf_page->Insert(key, value, comparator_);


    // 1. handle the case of duplicated keys
    if (!is_success) {

        // check if there is currently a bucket chain
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


            // insert into it
            assert(bucket->Insert(tmp_value));
            assert(bucket->Insert(value)); 


            // modify value at piar_index
            int key_index = leaf_page->KeyIndexGreaterEqual(key, comparator_);
            leaf_page->SetValueAt(key_index, RID(new_block_num, -1));
            buffer_manager_->UnpinBlock({index_file_name_, new_block_num}, true);
            buffer_manager_->UnpinBlock({index_file_name_, leaf_block_num}, true);
        }


        // there is a bucket num, try to insert into it.
        else {
            int first_bucket_num = tmp_value.GetBlockNum();
            InsertIntoBucketChain(first_bucket_num, value);
            buffer_manager_->UnpinBlock({index_file_name_, leaf_block_num}, false);
        }


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

        // get the middle key and will push up it
        KeyType middle_key = new_leaf_page->KeyAt(0);
        int leaf_page_parent_block_num = leaf_page->GetParentBlockNum();


        // release resource 
        buffer_manager_->UnpinBlock({index_file_name_, new_leaf_block_num}, true);
        buffer_manager_->UnpinBlock({index_file_name_, leaf_block_num}, true);
        

        // insert into parent directory
        InsertIntoParent(leaf_page_parent_block_num, middle_key,
                         leaf_block_num, new_leaf_block_num);
    }
    else {
        // since we have 
        buffer_manager_->UnpinBlock({index_file_name_, leaf_block_num}, true);
    }
    
    
}


INDEX_TEMPLATE_ARGUMENTS
template<class N>
N* BPLUSTREE_TYPE::Split(N* old_page) {
    int new_block_num;

    // handle leaf page
    if (old_page->GetPageType() == PageType::BPLUS_TREE_LEAF_PAGE) {
        LeafPage *new_leaf_page = CreateBTreePage<LeafPage>
                                  (PageType::BPLUS_TREE_LEAF_PAGE, &new_block_num);
    
        reinterpret_cast<LeafPage *>(old_page)->MoveHalfTo(new_leaf_page);
        return reinterpret_cast<N*>(new_leaf_page);
    }


    // handle directory page
    DirectoryPage *new_dir_page = CreateBTreePage<DirectoryPage>
                                  (PageType::BPLUS_TREE_DIRECTORY_PAGE, &new_block_num);
    
    reinterpret_cast<DirectoryPage *>(old_page)->MoveHalfTo(new_dir_page);
    return reinterpret_cast<N*>(new_dir_page);
}




INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(int dir_block_num, const KeyType &key,
                                      int left_block_num, int right_block_num) {
    
    // left child is the root_page, we should create a new root_page to replace it 
    if (dir_block_num == INVALID_BLOCK_NUM) {
        int new_root_block_num;
        DirectoryPage *parent_dir_page = CreateBTreePage<DirectoryPage>
                                         (PageType::BPLUS_TREE_DIRECTORY_PAGE, &new_root_block_num);

        // init new root of btree
        UpdateRootBlockNum(new_root_block_num);
        parent_dir_page->PopulateNewRoot(key, left_block_num, right_block_num);
    
        // redistributed parent block of child
        ResetDirChildParent(parent_dir_page);
        buffer_manager_->UnpinBlock({index_file_name_, new_root_block_num}, true);
        return;
    }
    

    // get parent block
    Buffer *dir_buffer = buffer_manager_->PinBlock({index_file_name_, dir_block_num});
    DirectoryPage *dir_page = reinterpret_cast<DirectoryPage*>(dir_buffer->contents()->GetRawDataPtr());
    assert(dir_page->GetSize() <= dir_page->GetMaxSize());


    
    // since directory_page tolerates size not splitting immediately when 
    // it reaches maxsize, it needs to be checked before insertion
    if (dir_page->GetSize() == dir_page->GetMaxSize()) {
        DirectoryPage *sibling_dir_page = Split<DirectoryPage>(dir_page);
        int sibling_dir_block_num = sibling_dir_page->GetBlockNum();
        int place_right = comparator_(key, sibling_dir_page->KeyAt(0));

        
        if (place_right == 1) {
            sibling_dir_page->InsertNode(key, right_block_num, comparator_);
            sibling_dir_page->MoveFirstToLast(dir_page);
        }
        else if (place_right == -1) {
            dir_page->InsertNode(key, right_block_num, comparator_);
        }
        else {
            SIMPLEDB_ASSERT(false, "the key of dir should unique");
        }


        // prevent the 'key' being the 'middle key'
        if (comparator_(dir_page->KeyAt(dir_page->GetSize() - 1), key) == 0) {
            assert(place_right == -1);
            // move 'key' to sibling_page
            dir_page->MoveLastToFirst(sibling_dir_page);
        }
        // make middle_key which exist in dir_page but null
        KeyType middle_key = dir_page->KeyAt(dir_page->GetSize() - 1);
        

        InsertIntoParent(dir_page->GetParentBlockNum(), middle_key, 
                         dir_block_num, sibling_dir_block_num);
        ResetDirChildParent(sibling_dir_page);


        buffer_manager_->UnpinBlock({index_file_name_, dir_block_num}, true);
        buffer_manager_->UnpinBlock({index_file_name_, sibling_dir_block_num}, true);
    }
    else {
        // don't need to split
        // insert the information of right_block into parent
        dir_page->InsertNode(key, right_block_num, comparator_);
        ResetDirChildParent(dir_page);
        buffer_manager_->UnpinBlock({index_file_name_, dir_block_num}, true);
    }

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







// ------------------------------------------------
// |           Insert relative Functions          |
// ------------------------------------------------
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N* BPLUSTREE_TYPE::CreateBTreePage(PageType page_type_, int *new_block_num) {
    // prefer to create a btree page from the deleted page
    Buffer *tmp_buffer = nullptr;

    if (last_deleted_num_ == INVALID_BLOCK_NUM) {
        tmp_buffer = buffer_manager_->NewBlock(index_file_name_, new_block_num);
    }
    else {
        tmp_buffer = buffer_manager_->PinBlock({index_file_name_, last_deleted_num_});
        *new_block_num = last_deleted_num_;
    }

    

    // reset infor
    switch (page_type_)
    {
    case PageType::BPLUS_TREE_LEAF_PAGE:
    {
        LeafPage *new_page = reinterpret_cast<LeafPage*>(tmp_buffer->contents()->GetRawDataPtr());
        new_page->Init(*new_block_num, max_leaf_size_);
        return reinterpret_cast<N*>(new_page);
    }


    case PageType::BPLUS_TREE_DIRECTORY_PAGE:
    {
        DirectoryPage *new_page = reinterpret_cast<DirectoryPage*>(tmp_buffer->contents()->GetRawDataPtr());
        new_page->Init(*new_block_num, max_dir_size_);
        return reinterpret_cast<N*>(new_page);
    }

    case PageType::BPLUS_TREE_BUCKET_PAGE:
    {
        BucketPage *new_page = reinterpret_cast<BucketPage*>(tmp_buffer->contents()->GetRawDataPtr());
        new_page->Init(*new_block_num);
        return reinterpret_cast<N*>(new_page);
    }
    
    default:
        assert(false);
        break;
    }


    return nullptr;
}


// template<class BPlusTreePage>
// void BPlusTree::DeleteBTreePage(BPlusTreePage* old_page, int block_num) {
//     // the flag is negative for ease of distinction
//     old_page->SetDeleteBlockList(block_num);
//     last_deleted_num_ = old_page->GetBlockID().BlockNum();
// }




INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ResetDirChildParent(DirectoryPage *dir_page) {
    int size = dir_page->GetSize();
    int parent_block_num = dir_page->GetBlockNum();

    for (int i = 0;i < size; i++) {
        int child_num = dir_page->ValueAt(i);
        auto *child_buffer = buffer_manager_->PinBlock({index_file_name_, child_num});
        auto *child_bplus_page = reinterpret_cast<BPlusTreePage*>
                                 (child_buffer->contents()->GetRawDataPtr());
        PageType page_type = child_bplus_page->GetPageType();


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

        buffer_manager_->UnpinBlock({index_file_name_, child_num}, true);
    }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(int block_num) {
    auto *buffer = buffer_manager_->PinBlock({index_file_name_, block_num});
    auto *bplus_page = reinterpret_cast<BPlusTreePage*>
                             (buffer->contents()->GetRawDataPtr());
    auto *dir_page = reinterpret_cast<DirectoryPage*>(bplus_page);

    std::cout << "start   print  tree   " << std::endl; 

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


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace SimpleDB



#endif