//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include <iostream>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  /**
   * A frame inside the LRUK Replacer.
   */
  class Frame {
   public:
    explicit Frame(frame_id_t frame_id, size_t list_capacity, LRUKReplacer* replacer);

    DISALLOW_COPY_AND_MOVE(Frame);

    /**
     * @brief Insert a timestamp to the front of access_timestamp_list_, 
     * remove the element at the back if size exceeds k_.
     * @param timestamp 
     */
    void InsertTimestamp(size_t timestamp);

    /** @brief Get the frame id. */
    inline auto GetFrameId() const -> frame_id_t {return frame_id_;}

    /** @brief Check if the frame is evictable. */
    inline auto IsEvictable() const -> bool {return evictable_;}

    /** @brief Set the evictable_ status of the frame. */
    inline void SetFrameEvictable(bool set_evictable) {evictable_ = set_evictable;}

    /** @brief Check if the frame has at least k access timestamps. */
    inline auto HasAtLeastKTimestamps() const -> bool {return access_timestamp_list_.size() == list_capacity_;}

    /** @brief Get backward k-distance of the frame. */
    inline auto GetKDistance() const -> size_t {return replacer_->GetTimestamp() - access_timestamp_list_.back();}

   private:
    frame_id_t frame_id_;
    // Insert new timestamp to the front.
    // Contains k_ most recent access timestamps.
    std::list<size_t> access_timestamp_list_;
    size_t list_capacity_; // k_
    bool evictable_;

    LRUKReplacer* replacer_;
  };

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t current_timestamp_{0}; // Logical timestamp, increment by 1 for every access
  size_t curr_size_{0}; // The current number of evictable frames
  size_t replacer_size_; // The max number of frames
  size_t k_;
  mutable std::mutex latch_;

  /**
   * A doubly linked list containing all frames.
   * Always evict frame from front of the list.
   * 
   * The list can be divided into 2 parts:
   * 1. The part near the front contains frames with less than k access timestamps
   * which should be evicted first, ordered based on "k-distance".
   * 2. The part near the back contains frames with at least k access timestamps,
   * ordered based on k-distance.
   */
  std::list<std::shared_ptr<Frame>> frame_list_;

  // Maps frame id to Frame for fast lookup, store iterator as key
  std::unordered_map<frame_id_t, std::list<std::shared_ptr<Frame>>::iterator> frame_map_;

  /**
   * @brief Get the current logical timestamp
   * 
   * @return size_t The current logical timestamp
   */
  size_t GetTimestamp() {
    return current_timestamp_;
  }

  /**
   * @brief Get the current logical timestamp and increment it by 1
   * @return size_t The current logical timestamp
   */
  size_t GetAndIncrementTimestamp() {
    return current_timestamp_++;
  }
};

}  // namespace bustub
