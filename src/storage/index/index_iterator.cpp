/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_ == nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index iterator out of range");
  }
  return leaf_page_->KeyValueAt(array_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }

  if (array_index_ < leaf_page_->GetSize() - 1) {
    array_index_++;
    return *this;
  }

  if (leaf_page_->GetNextPageId() == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    leaf_page_ = nullptr;
    array_index_ = 0;
    return *this;
  }

  LeafPage *next_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId()));
  buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  leaf_page_ = next_page;
  array_index_ = 0;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
