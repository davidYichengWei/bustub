//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FindFreeFrame(frame_id_t &frame_id) -> bool {
  // Try use a free frame
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {  // Try evict a frame if no free frame
    if (!replacer_->Evict(&frame_id)) {
      return false;
    }
    // Write the replacement page back to the disk if it's dirty
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    // Remove the replacement page from the page table
    page_table_->Remove(pages_[frame_id].page_id_);
    // Reset the page
    pages_[frame_id].ResetMemory();
    pages_[frame_id].ResetMetadata();
  }

  return true;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = pool_size_;
  if (!FindFreeFrame(frame_id)) {
    return nullptr;
  }

  // Allocate a new page on the disk in the buffer pool
  *page_id = AllocatePage();
  pages_[frame_id].page_id_ = *page_id;

  // Set the page table and the replacer
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = pool_size_;
  // Check if already in the buffer pool
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }

  // Find a free frame and read the page from the disk
  if (!FindFreeFrame(frame_id)) {
    return nullptr;
  }
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }

    disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  // Remove the page from the page table
  if (!page_table_->Remove(page_id)) {
    return false;
  }
  // Remove the page from the replacer
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  // Reset the page's memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].ResetMetadata();
  // Deallocate the page on the disk
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
