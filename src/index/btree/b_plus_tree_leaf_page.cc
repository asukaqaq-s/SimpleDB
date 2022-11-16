#ifndef BPLUS_TREE_LEAF_PAGE_CC
#define BPLUS_TREE_LEAF_PAGE_CC

#include "index/btree/b_plus_tree_leaf_page.h"

#include <iostream>


namespace SimpleDB {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int block_num, int max_size, int parent_block_num) {

    SetPageType(PageType::BPLUS_TREE_LEAF_PAGE);
    SetMaxSize(max_size);
    SetParentBlockNum(parent_block_num);
    SetBlockNum(block_num);
    SetSize(0);
    SetNextBlockNum(INVALID_BLOCK_NUM);
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextBlockNum() const {
    return next_block_num_;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextBlockNum(int next_page_id) {
    next_block_num_ = next_page_id;
}


INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
    return array_[index].first;
}



INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const {
    return array_[index].second;
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, KeyType key) {
    array_[index].first = key;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, ValueType value) {
    array_[index].second = value;
}



INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndexGreaterEqual
(const KeyType &key, const KeyComparator &comparator) const {
    int left = 0, right = GetSize() - 1;

    while (left < right) {
        int mid = (left + right) / 2;
        auto tmp_key = KeyAt(mid);
        
        if (comparator(tmp_key, key) < 0) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }

    return left;
}



INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndexGreaterThan
(const KeyType &key, const KeyComparator &comparator) const {
    int left = 0, right = GetSize() - 1;

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

    return left;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
    return array_[index];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
    if (GetSize() == 0) {
        array_[0] = std::make_pair(key, value);
    } else {

        // binary search to find the first index which 
        // array[index].first >= key
        int left = 0, right = GetSize();
        while (left < right) {
            int mid = (left + right) / 2;
            auto tmp_key = KeyAt(mid);

            if (comparator(tmp_key, key) < 0) {
                left = mid + 1;
            }
            else {
                right = mid;
            }
        }


        // if key is exist, return false to insert into bucket
        if (comparator(KeyAt(left), key) == 0) {
            return false;
        }

        
        for (auto i = GetSize(); i >= left; i--) {
            array_[i] = array_[i - 1];
        }
        array_[left] = std::make_pair(key, value);
    }

    IncreaseSize(1);
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
    auto index = KeyIndexGreaterEqual(key, comparator);
    if (comparator(array_[index].first, key) == 0) {
        if (value != nullptr) {
            *value = array_[index].second;
        }
        return true;
    }
    
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
    auto index = KeyIndexGreaterEqual(key, comparator);
    if (comparator(array_[index].first, key) == 0) {
        for (auto i = index; i < GetSize() - 1; i++) {
            array_[i] = array_[i + 1];
        }
        IncreaseSize(-1);
        return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
    
    // since the key in the last position does not store anything, it is rounded down
    int half = (GetSize()) / 2;
    recipient->CopyNFrom(&array_[half], GetSize() - half);
    SetSize(half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
    recipient->CopyNFrom(array_, GetSize());
    recipient->SetNextBlockNum(GetNextBlockNum());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
    recipient->CopyLastFrom(array_[0]);
    IncreaseSize(-1);
    for (int i = 0; i < GetSize(); i++) {
        array_[i] = array_[i + 1];
    }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
    recipient->CopyFirstFrom(array_[GetSize() - 1]);
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
    for (auto i = 0; i < size; i++) {
        array_[i + GetSize()] = items[i];
    }
    IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array_[GetSize()] = item;
    IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    for (auto i = GetSize(); i >= 1; i--) {
        array_[i] = array_[i - 1];
    }
    array_[0] = item;
    IncreaseSize(1);
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace SimpleDB


#endif