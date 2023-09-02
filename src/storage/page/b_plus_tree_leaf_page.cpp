//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define ARRAY_INDEX_CHECK if (index < 0 || index >= GetMaxSize()) throw std::out_of_range("index out of range")

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  ARRAY_INDEX_CHECK;
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ARRAY_INDEX_CHECK;
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyValueAt(int index) -> MappingType &{
  ARRAY_INDEX_CHECK;
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  ARRAY_INDEX_CHECK;
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertKeyValuePair(const KeyType &key, const ValueType &value, 
                                                    const KeyComparator &comparator) -> bool {
  int max_size = GetMaxSize();
  if (GetSize() >= max_size) {
    throw std::logic_error("Leaf node is full before insert");
  }

  // Perform binary search to find the index to insert
  int index = GetSize();
  int left = 0, right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      return false;
    }
    else if (comparator(array_[mid].first, key) > 0) {
      index = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  // Shift elements in the range of [index, max_size_ - 1) to the right by 1
  // Need to use copy_backward to copy in reverse order
  std::copy_backward(array_ + index, array_ + max_size - 1, array_ + max_size);

  // Insert the kv pari at index
  SetKeyValueAt(index, key, value);
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SplitData(B_PLUS_TREE_LEAF_PAGE_TYPE *destination_page) {
  int mid_index = GetMinSize();
  for (int i = mid_index, j = 0; i < GetMaxSize(); i++, j++) {
    destination_page->SetKeyValueAt(j, array_[i].first, array_[i].second);
  }
  SetSize(mid_index);
  destination_page->IncreaseSize(GetMaxSize() - mid_index);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
