#ifndef BPLUS_TREE_DIRECTORY_PAGE_CC
#define BPLUS_TREE_DIRECTORY_PAGE_CC

#include "index/btree/b_plus_tree_directory_page.h"


namespace SimpleDB {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::Init(int block_num, int max_size, int parent_id) {
    SetLsn(INVALID_LSN);
    SetPageType(PageType::BPLUS_TREE_DIRECTORY_PAGE);
    SetParentBlockNum(parent_id);
    SetMaxSize(max_size);
    SetBlockNum(block_num);
    SetSize(0);
}


INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_DIRECTORY_PAGE_TYPE::KeyAt(int index) const {
    return array_[index].first;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    array_[index].first = key;
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_DIRECTORY_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); i++) {
        if (array_[i].second == value) {
            return i;
        }
    }
    return -1;
}



INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_DIRECTORY_PAGE_TYPE::ValueAt(int index) const {
    return array_[index].second;
}



INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_DIRECTORY_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
    // binary search the first key which greater than search_key 
    // end to GetSize() - 1 
    int left = 0, right = GetSize() - 2;
    while (left < right) {
        int mid = (left + right) / 2;
        auto tmp_key = KeyAt(mid);

        if (comparator(tmp_key, key) <= 0) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }


    if (comparator(key, KeyAt(left)) >= 0 ) {
        assert(left == GetSize() - 2);
        left = GetSize() - 1;
    }


    return ValueAt(left);
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::PopulateNewRoot(const KeyType &key, 
                                                      const ValueType &left_child,
                                                      const ValueType &right_child) {
    array_[0] = std::make_pair(key, left_child);
    array_[1].second = right_child;

    SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::InsertNode(const KeyType &new_key, 
                                                 const ValueType &new_value, 
                                                 const KeyComparator &comparator) {
    // binary search to find the first key which greater than new_key
    int left = 0, right = GetSize() - 2;
    while (left < right) {
        int mid = (left + right) / 2;
        auto tmp_key = KeyAt(mid);

        if (comparator(tmp_key, new_key) <= 0) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }


    // check if greater than all keys, then insert it into the last position    
    if (comparator(new_key, KeyAt(left)) >= 0 ) {
        assert(left == GetSize() - 2);
        left = GetSize() - 1;
    }


    
    // move later left forward
    int size = GetSize();
    for (int i = left; i < size; i++) {
        array_[i + 1] = array_[i];
    }

    // since a[left + 1].second point to the page with all keys less than new_key
    // so a[left].second update to a[left + 1].second and a[left + 1].second
    // update to new_value.
    array_[left].first= new_key;
    array_[left].second = array_[left + 1].second;
    array_[left + 1].second = new_value;
    IncreaseSize(1);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::RemoveAt(int index) {
    for (int i = index; i < GetSize() - 1; i++) {
        array_[i] = array_[i + 1];
    }
    IncreaseSize(-1);
}


INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_DIRECTORY_PAGE_TYPE::RemoveAllChild() {
    SetSize(0);
    return array_[0].second;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::MoveAllTo(BPlusTreeDirectoryPage *recipient) {
    recipient->CopyNFrom(array_, GetSize());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::MoveHalfTo(BPlusTreeDirectoryPage *recipient) {    
    uint half = (GetSize() - 1) / 2;
    recipient->CopyNFrom(&array_[half], GetSize() - half);
    SetSize(half);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::MoveLastToFirst(BPlusTreeDirectoryPage *recipient) {
    MappingType last = array_[GetSize() - 1];
    recipient->CopyFirstFrom(last);
    IncreaseSize(-1);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::MoveFirstToLast(BPlusTreeDirectoryPage *recipient) {
    MappingType first = array_[0];
    recipient->CopyLastFrom(first);
    RemoveAt(0);
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::CopyNFrom(MappingType *items, uint32_t size) {
    for (uint i = 0; i < size; i++) {
        array_[GetSize() + i] = items[i];
    }
    IncreaseSize(size);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array_[GetSize()] = item;
    IncreaseSize(1);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    int size = GetSize();
    for (int i = size; i >= 1; i--) {
        array_[i] = array_[i - 1];
    }


    array_[0].first= item.first;
    array_[0].second = array_[1].second;
    array_[1].second = item.second;
    IncreaseSize(1);
}


template class BPlusTreeDirectoryPage<GenericKey<4>, int, GenericComparator<4>>;
template class BPlusTreeDirectoryPage<GenericKey<8>, int, GenericComparator<8>>;
template class BPlusTreeDirectoryPage<GenericKey<16>, int, GenericComparator<16>>;
template class BPlusTreeDirectoryPage<GenericKey<32>, int, GenericComparator<32>>;
template class BPlusTreeDirectoryPage<GenericKey<64>, int, GenericComparator<64>>;

} // namespace SimpleDB


#endif