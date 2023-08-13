//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // Find the first evictable frame in frame_list_ to evict
  for (auto frame : frame_list_) {
    if (frame->IsEvictable()) {
      frame_map_.erase(frame->GetFrameId());
      *frame_id = frame->GetFrameId();
      frame_list_.remove(frame);
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");
  std::scoped_lock<std::mutex> lock(latch_);

  // Create a new Frame if frame_id is not present
  if (frame_map_.find(frame_id) == frame_map_.end()) {
    std::shared_ptr<Frame> new_frame = std::make_shared<Frame>(frame_id, k_, this);
    frame_list_.push_front(new_frame);
    frame_map_[frame_id] = frame_list_.begin();
    curr_size_++;
  }

  // Update access_timestamp_list_
  auto frame_iterator = frame_map_[frame_id];
  (*frame_iterator)->InsertTimestamp(GetAndIncrementTimestamp());

  // Determine the number of positions to move the frame backward
  int move_count = 0;
  auto frame_to_compare = frame_iterator;
  frame_to_compare++;
  while (frame_to_compare != frame_list_.end()) {
    if (!(*frame_iterator)->HasAtLeastKTimestamps() && (*frame_to_compare)->HasAtLeastKTimestamps()) {
      break;
    }
    if ((*frame_iterator)->HasAtLeastKTimestamps() && (*frame_to_compare)->HasAtLeastKTimestamps() &&
        (*frame_iterator)->GetKDistance() > (*frame_to_compare)->GetKDistance()) {
      break;
    }
    move_count++;
    frame_to_compare++;
  }

  // Move the frame in frame_list_
  auto target_position = frame_iterator;
  std::advance(target_position, move_count);
  frame_list_.splice(target_position, frame_list_, frame_iterator);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_map_.find(frame_id) == frame_map_.end()) {
    return;
  }

  std::shared_ptr<Frame> frame = *frame_map_[frame_id];
  if (frame->IsEvictable() && !set_evictable) {
    frame->SetFrameEvictable(set_evictable);
    curr_size_--;
  } else if (!frame->IsEvictable() && set_evictable) {
    frame->SetFrameEvictable(set_evictable);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_map_.find(frame_id) == frame_map_.end()) {
    return;
  }

  auto frame_iterator = frame_map_[frame_id];
  BUSTUB_ASSERT((*frame_iterator)->IsEvictable(), "Frame not evictable");

  frame_map_.erase(frame_id);
  frame_list_.erase(frame_iterator);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

//===--------------------------------------------------------------------===//
// Frame
//===--------------------------------------------------------------------===//

LRUKReplacer::Frame::Frame(frame_id_t frame_id, size_t list_capacity, LRUKReplacer *replacer)
    : frame_id_(frame_id), list_capacity_(list_capacity), evictable_(true), replacer_(replacer) {}

void LRUKReplacer::Frame::InsertTimestamp(size_t timestamp) {
  if (access_timestamp_list_.size() >= list_capacity_) {
    access_timestamp_list_.pop_back();
  }
  access_timestamp_list_.emplace_front(timestamp);
}

}  // namespace bustub
