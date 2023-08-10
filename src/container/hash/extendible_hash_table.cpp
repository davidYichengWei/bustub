//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

// This pattern of separating the internal and external implementations of a function is common in concurrent code.
// Because the internal function might be called by other functions who already acquired the latch
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_); // scoped_lock antuoatically unlocks when it goes out of scope
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  std::shared_ptr<Bucket> bucket = dir_[IndexOf(key)];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  std::shared_ptr<Bucket> bucket = dir_[IndexOf(key)];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  V v; // Dummy V for calling Find
  if (dir_[IndexOf(key)]->Find(key, v)) {
    dir_[IndexOf(key)]->Insert(key, value);
    return;
  }

  while (dir_[IndexOf(key)]->IsFull()) {
    std::shared_ptr<Bucket> old_bucket = dir_[IndexOf(key)];
    
    // 1. If the local depth of the bucket is equal to the global depth, 
    // increment the global depth and double the size of the directory.
    if (global_depth_ == old_bucket->GetDepth()) {
      global_depth_++;
      dir_.reserve(dir_.size() * 2);
      // Arguments: insert position, begin iterator, end iterator
      dir_.insert(dir_.end(), dir_.begin(), dir_.end());
    }

    // 2. Increment the local depth of the bucket
    old_bucket->IncrementDepth();

    // 3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket
    std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, old_bucket->GetDepth());
    num_buckets_++;

    // Redistribute kv pairs
    std::list<std::pair<K, V>>& old_bucket_item_list = old_bucket->GetItems(); // Use reference
    std::list<std::pair<K, V>>& new_bucket_item_list = new_bucket->GetItems();
    // Use bitwise & to check the new bit taken into account by the bucket
    int mask = 1 << (old_bucket->GetDepth() - 1); 
    for (auto it = old_bucket_item_list.begin(); it != old_bucket_item_list.end();) {
      size_t key_hash = std::hash<K>()(it->first);
      if (static_cast<bool>(key_hash & mask)) {
        new_bucket_item_list.push_back(std::move(*it));
        // erase invalidates it, need to assign the return value of erase back to it,
        // which points to the element after the deleted one
        it = old_bucket_item_list.erase(it); 
      }
      else {
        it++;
      }
    }

    // Redistribute directory pointers
    size_t i = 0;
    while (i < dir_.size() && dir_[i] != old_bucket) {
      i++;
    }
    while (i < dir_.size()) {
      if (dir_[i] == old_bucket && static_cast<bool>(i & mask)) {
        dir_[i] = new_bucket;
      }
      // mask is 2^(new_local_depth - 1), which is the distance between 2 cells containing the same bucket
      i += mask; 
    }
  }

  dir_[IndexOf(key)]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (key == k) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull())
    return false;
  
  for (auto &[k, v] : list_) {
    if (key == k) {
      v = value;
      return true;
    }
  }

  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
