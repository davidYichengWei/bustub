//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  /**
   * @brief Check if the B+ tree is empty
   * 
   * @return true if the B+ tree contains no key-value pairs
   * @return false otherwise
   */
  auto IsEmpty() const -> bool;

  /**
   * @brief Insert a key-value pair into the B+ tree.
   * If current tree is empty, start new tree, update root page id and insert
   * entry, otherwise insert into leaf page.
   * 
   * You should correctly perform splits if insertion triggers the splitting condition 
   * (number of key/value pairs AFTER insertion equals to max_size for leaf nodes, 
   * number of children BEFORE insertion equals to max_size for internal nodes). 
   * Since any write operation could lead to the change of root_page_id in B+Tree index, 
   * it is your responsibility to update root_page_id in the header page
   * 
   * @param key the key to insert
   * @param value the value to insert
   * @param transaction ignore for project 2
   * @return true if insert successfully
   * @return false if the key already exists
   */
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  /**
   * @brief Remove a kv pair from the B+ tree.
   * If the key is not found, just return.
   * After removing the kv pair from the leaf page, if the leaf page's size is smaller than
   * its min_size, need to perform redistribute or merge.
   * 
   * 
   * @param key 
   * @param transaction 
   */
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  /**
   * @brief Get the record id associated with a given key
   * 
   * @param key the key to search for
   * @param[out] result a vector container to store the record id
   * @param transaction ignore for project 2
   * @return true key is found
   * @return false key not found
   */
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  /**
   * @brief Find the leaf page that might contain the key
   * 
   * @param key the key to search for
   * @return LeafPage* a pointer to the leaf page
   */
  auto FindLeafPage(const KeyType &key) const -> LeafPage *;

  /**
   * @brief Split the leaf page by creating a new leaf page and moving the second half of the key-value pairs to the new page.
   * Need to set the next_page_id_ attribute of the old page.
   * 
   * @param leaf_page the leaf page to be splitted
   * @return LeafPage* a pointer to the new leaf page, remember to Unpin it later!
   */
  auto SplitLeafPage(LeafPage *leaf_page) -> LeafPage *;

  /**
   * @brief Split the internal page by creating a new internal page and moving the second half of the key-value pairs to the new page.
   * Need to update the parent_page_id of all pages under the new internal page.
   * 
   * @param internal_page the internal page to be splitted
   * @param kv_pairs_after_insert a vector containing sorted kv pairs after insertion
   * @return InternalPage* a pointer to the new internal page, remember to Unpin it later!
   */
  auto SplitInternalPage(InternalPage *internal_page, std::vector<std::pair<KeyType, page_id_t>> kv_pairs_after_insert) -> InternalPage *;

  /**
   * @brief Insert a kv pair (right_page_key, right_page_id) into an internal page.
   * Case 1: internal_page_id is INVALID_PAGE_ID 
   * - Create a new internal page, set it as the new root.
   * - Insert both left_page_id and the kv pair.
   * - Update the parent_page_id field of both left page and right page.
   * - Unpin all 3 pages.
   * 
   * Case 2: page with internal_page_id is not full
   * - Insert the right page kv pair in sorted order.
   * - Unpin the internal page.
   * 
   * Case 3: page with internal_page_id is full
   * - Split the internal page.
   * - Call InsertIntoInternalPage recursively to insert the relevant information, where the 
   * left_page_id is the old internal_page_id, right_page_key is the key at index 0 of the new
   * right page, and right_page_id is the id of the new right page. Here copying up and pushing 
   * up the middle key is the same operation, as we are not using the key at index 0 of an internal page.
   * - Insert the original kv pair into the correct internal page after the split.
   * - Unpin internal_page and new_internal_page.
   * 
   * @param internal_page_id id of the target internal page
   * @param left_page_id the id of the left page after split
   * @param right_page_key the key at index 0 of the right page after split
   * @param right_page_id the id of the right page after split
   */
  void InsertIntoInternalPage(page_id_t internal_page_id, page_id_t left_page_id, KeyType right_page_key, page_id_t right_page_id);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
