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

#define ARRAY_INDEX_CHECK if (index < 0 || index >= GetSize()) throw std::out_of_range("index out of range")

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
  if (index < 0 || index >= GetMaxSize()) throw std::out_of_range("index out of range");
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index < 0 || index >= GetMaxSize()) throw std::out_of_range("index out of range");
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyValueAt(int index, const KeyType &key, const ValueType &value) {
  if (index < 0 || index >= GetMaxSize()) throw std::out_of_range("index out of range");
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

  // Insert the kv pair at index
  SetKeyValueAt(index, key, value);
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToTmpVector(const KeyType &key, const ValueType &value, 
                                                       const KeyComparator &comparator) -> std::vector<MappingType> {
  if (GetSize() != GetMaxSize()) {
    throw std::runtime_error("InsertToTmpVector: size is not max_size");
  }
  
  // One more space for the new kv pair
  std::vector<MappingType> result(GetMaxSize() + 1);
  std::copy(array_, array_ + GetMaxSize(), result.begin());

  // Perform binary search to find the index to insert
  int index = GetMaxSize();
  int left = 1, right = GetMaxSize() - 1; // 0th key is invalid
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(result[mid].first, key) == 0) {
      throw std::logic_error("Duplicate key in internal page");
    }
    else if (comparator(result[mid].first, key) > 0) {
      index = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  // Shift elements in the range of [index, max_size_) to the right by 1
  // Need to use copy_backward to copy in reverse order
  std::copy_backward(result.begin() + index, result.begin() + GetMaxSize(), result.begin() + GetMaxSize() + 1);

  // Insert the kv pair at index
  result[index].first = key;
  result[index].second = value;

  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKeyValuePair(ValueType value_to_remove) -> bool {
  int index = -1;
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value_to_remove) {
      index = i;
      break;
    }
  }
  assert(index != -1);

  // Just overwrite data
  std::copy(array_ + index + 1, array_ + GetMaxSize(), array_ + index);
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFromLeftSibling(B_PLUS_TREE_INTERNAL_PAGE_TYPE *left_sibling, 
                                                          const KeyComparator &comparator) -> bool {
  if (left_sibling->GetSize() <= left_sibling->GetMinSize()) return false;

  // Steal the last element from left_sibling
  if (!PrependKeyValuePair(left_sibling->KeyAt(left_sibling->GetSize() - 1), 
                           left_sibling->ValueAt(left_sibling->GetSize() - 1), comparator)) {
    throw std::runtime_error("Insertion failed when stealing from left sibling leaf page");
  }

  // Remove the last element in left_sibling
  left_sibling->DecreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFromRightSibling(B_PLUS_TREE_INTERNAL_PAGE_TYPE *right_sibling, 
                                                           const KeyComparator &comparator) -> bool {
  if (right_sibling->GetSize() <= right_sibling->GetMinSize()) return false;

  // Steal the first element from right_sibling
  if (!InsertKeyValuePair(right_sibling->KeyAt(0), right_sibling->ValueAt(0), comparator)) {
    throw std::runtime_error("Insertion failed when stealing from left sibling leaf page");
  }

  // Remove the first element in right_sibling
  if (!right_sibling->RemoveKeyValuePair(right_sibling->ValueAt(0))) {
    throw std::runtime_error("Failed to remove the first element in right_sibling");
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(B_PLUS_TREE_INTERNAL_PAGE_TYPE *right_sibling) -> bool {
  // If merge is required, shouldn't need to split after merge
  if (GetSize() + right_sibling->GetSize() >= GetMaxSize()) {
    return false;
  }

  std::copy(right_sibling->array_, right_sibling->array_ + right_sibling->GetSize(), array_ + GetSize());
  IncreaseSize(right_sibling->GetSize());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PrependKeyValuePair(KeyType key, ValueType value, 
                                                         const KeyComparator &comparator) -> bool {
  if (GetSize() >= GetMaxSize()) {
    return false;
  }

  std::copy_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  SetKeyValueAt(0, key, value);
  IncreaseSize(1);
  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
