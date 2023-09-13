//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator() : leaf_page_(nullptr), array_index_(0), buffer_pool_manager_(nullptr) {}
  IndexIterator(LeafPage *leaf_page, int array_index, BufferPoolManager *buffer_pool_manager)
    : leaf_page_(leaf_page), array_index_(array_index), buffer_pool_manager_(buffer_pool_manager) {}
  ~IndexIterator() {
    if (leaf_page_) {
      // Unlatch
      Page *page = buffer_pool_manager_->FetchPage(leaf_page_->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false); // decrement pin count twice
      buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    }
  }

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return itr.leaf_page_ == leaf_page_ && itr.array_index_ == array_index_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return itr.leaf_page_ != leaf_page_ || itr.array_index_ != array_index_; }

 private:
  // add your own private member variables here
  LeafPage *leaf_page_;
  int array_index_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
