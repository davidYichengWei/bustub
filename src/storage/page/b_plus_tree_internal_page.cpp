//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

#define ARRAY_INDEX_CHECK if (index < 0 || index >= GetMaxSize()) throw std::out_of_range("index out of range")

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  ARRAY_INDEX_CHECK;
  return array_[index].first;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ARRAY_INDEX_CHECK;
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  ARRAY_INDEX_CHECK;
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  ARRAY_INDEX_CHECK;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  ARRAY_INDEX_CHECK;
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyValuePair(const KeyType &key, const ValueType &value, 
                                                    const KeyComparator &comparator) -> bool {
  int max_size = GetMaxSize();
  if (GetSize() >= max_size) {
    return false;
  }

  // Perform binary search to find the index to insert
  int index = GetSize();
  int left = 1, right = GetSize() - 1; // 0th key is invalid
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      throw std::logic_error("Duplicate key in internal page");
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

// Test 1:
// size = 6
// data: null-A, 3-B, 5-C, 7-D, 9-E, 11-F
// mid_index = 3
// after split:
// old_data: null-A, 3-B, 5-C, old_size: 3
// new_data: 7-D, 9-E, 11-F, new_size: 6 - 3 = 3

// Test 2:
// size = 7
// data: null-A, 3-B, 5-C, 7-D, 9-E, 11-F, 13-G
// mid_index = 3
// after split:
// old_data: null-A, 3-B, 5-C, old_size: 3
// new_data: 7-D, 9-E, 11-F, 13-G new_size: 7 - 3 = 4
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitData(B_PLUS_TREE_INTERNAL_PAGE_TYPE *destination_page) {
  int mid_index = GetMinSize();
  for (int i = mid_index, j = 0; i < GetMaxSize(); i++, j++) {
    destination_page->SetKeyValueAt(j, array_[i].first, array_[i].second);
  }
  SetSize(mid_index);
  destination_page->IncreaseSize(GetMaxSize() - mid_index);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
