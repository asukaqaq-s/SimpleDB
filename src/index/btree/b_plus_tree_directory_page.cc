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
int B_PLUS_TREE_DIRECTORY_PAGE_TYPE::KeyIndex(const KeyType &key, 
                                              const KeyComparator &comparator) const {
    // binary search the first key which greater equal than search_key 
    // end to GetSize() - 1 
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
ValueType B_PLUS_TREE_DIRECTORY_PAGE_TYPE::ValueAt(int index) const {
    return array_[index].second;
}



INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_DIRECTORY_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
    // binary search the last key which less equal than key
    int left = 0, right = GetSize() - 1;
    while (left < right) {
        int mid = (left + right + 1) / 2;
        auto tmp_key = KeyAt(mid);

        if (comparator(tmp_key, key) > 0) {
            right = mid - 1;
        }
        else {
            left = mid;
        }
    }

    return ValueAt(left);
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::PopulateNewRoot(const ValueType &left_child, 
                                                     const KeyType &key, 
                                                     const ValueType &right_child) {
    IncreaseSize(1);
    array_[0].second = left_child;

    // before | pointer 0: old_value | key 0: null    | pointer 1: null      | key 1: null |
    InsertNodeAfter(left_child, key, right_child);
    // after  | pointer 0: old_value | key 0: null | pointer 1: new_value | key 1: key |

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, 
                                                      const KeyType &new_key, 
                                                      const ValueType &new_value) {
    for (uint i = GetSize(); i >= 1; i--) {
        if (array_[i - 1].second == old_value) {
            array_[i] = std::make_pair(new_key, new_value);
            break;
        }
        array_[i] = array_[i - 1];
    }
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
    uint half = (GetSize() + 1) / 2;
    recipient->CopyNFrom(&array_[half], GetSize() - half);
    SetSize(half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeDirectoryPage *recipient) {
    recipient->CopyLastFrom(array_[0]);
    RemoveAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeDirectoryPage *recipient) {
    recipient->CopyFirstFrom(array_[GetSize() - 1]);
    RemoveAt(GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::CopyNFrom(MappingType *items, uint32_t size) {
    for (uint i = 0; i < size; i++) {
        array_[GetSize() + i] = items[i];
    }
    IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::CopyLastFrom(const MappingType &pair) {
    array_[GetSize()] = pair;
    IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_DIRECTORY_PAGE_TYPE::CopyFirstFrom(const MappingType &pair) {
    for (uint i = GetSize(); i >= 1; i--) {
        array_[i] = array_[i - 1];
    }
    array_[0] = pair;
    IncreaseSize(1);
}



template class BPlusTreeDirectoryPage<GenericKey<4>, int, GenericComparator<4>>;
template class BPlusTreeDirectoryPage<GenericKey<8>, int, GenericComparator<8>>;
template class BPlusTreeDirectoryPage<GenericKey<16>, int, GenericComparator<16>>;
template class BPlusTreeDirectoryPage<GenericKey<32>, int, GenericComparator<32>>;
template class BPlusTreeDirectoryPage<GenericKey<64>, int, GenericComparator<64>>;

} // namespace SimpleDB


#endif