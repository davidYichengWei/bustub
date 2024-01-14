//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  ValidateLockTableInput(txn, lock_mode);

  // Get the LockRequestQueue for the table
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.at(oid);
  table_lock_map_latch_.unlock();

  // Insert the lock request into the queue
  {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_); // Note: might need to get this lock before releasing the table_lock_map_latch_
    bool upgrade = false;

    // Traverse the queue to check if the txn already has a lock
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); /** no increment here */) {
      LockRequest *existing_request = it->get();
      if (existing_request->txn_id_ == txn->GetTransactionId()) {
        // granted_ must be true, otherwise the txn cannot issue a second lock request
        assert(existing_request->granted_);

        if (existing_request->lock_mode_ == lock_mode) {
          return true;
        }

        // Abort if another transaction is doing a lock upgrade
        if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
          AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
        }

        // Lock upgrade
        LockMode existing_lock_mode = existing_request->lock_mode_;
        ValidateLockUpgrade(existing_lock_mode, lock_mode, txn);

        // Mark the queue as being upgraded by this txn
        lock_request_queue->upgrading_ = txn->GetTransactionId();
        upgrade = true;

        // Remove the existing request from the queue
        it = lock_request_queue->request_queue_.erase(it);
        // Update the lock count and the txn's lock set
        switch (existing_lock_mode) {
          case LockMode::SHARED:
            lock_request_queue->s_lock_count_--;
            txn->GetSharedTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_SHARED:
            lock_request_queue->is_lock_count_--;
            txn->GetIntentionSharedTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            lock_request_queue->ix_lock_count_--;
            txn->GetIntentionExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            lock_request_queue->six_lock_count_--;
            txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
            break;
          default:
            throw std::runtime_error("Invalid lock mode");
        }
      }
      else {
        it++;
      }
    }

    // Create a new LockRequest
    std::unique_ptr<LockRequest> lock_request = std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

    // If the txn is upgrading, then we need to insert the lock request into the front of the queue
    if (upgrade) {
      lock_request_queue->request_queue_.push_front(std::move(lock_request));
    }
    else {
      lock_request_queue->request_queue_.push_back(std::move(lock_request));
    }
  }

  // Wait for the lock to be granted
  {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    while (!GrantTableLock(txn, lock_mode, lock_request_queue.get())) {
      lock_request_queue->cv_.wait(lock);
    }
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  ValidateUnlockTableInput(txn, oid);

  // Get the LockRequestQueue for the table
  table_lock_map_latch_.lock();
  auto lock_request_queue = table_lock_map_.at(oid);
  table_lock_map_latch_.unlock();

  {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    // Traverse the queue to find the txn's lock request
    bool found_request_in_queue = false;
    auto it = lock_request_queue->request_queue_.begin();
    while (it != lock_request_queue->request_queue_.end()) {
      LockRequest *existing_request = it->get();
      if (existing_request->txn_id_ == txn->GetTransactionId()) {
        found_request_in_queue = true;
        break;
      }
      ++it;
    }
    if (!found_request_in_queue) {
      AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    // Transaction state transition
    LockMode lock_mode = it->get()->lock_mode_;
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::REPEATABLE_READ:
          if (txn->GetState() == TransactionState::GROWING)
            txn->SetState(TransactionState::SHRINKING);
          break;
        case IsolationLevel::READ_COMMITTED:
          if (lock_mode == LockMode::EXCLUSIVE) {
            if (txn->GetState() == TransactionState::GROWING)
              txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_mode == LockMode::EXCLUSIVE) {
            if (txn->GetState() == TransactionState::GROWING)
              txn->SetState(TransactionState::SHRINKING);
          }
          break;
        default:
          throw std::runtime_error("Invalid isolation level");
      }
    }

    // Remove the LockRequest from the queue
    lock_request_queue->request_queue_.erase(it);

    // Update the lock counts and the txn's lock set
    switch (lock_mode) {
      case LockMode::SHARED:
        lock_request_queue->s_lock_count_--;
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case LockMode::EXCLUSIVE:
        lock_request_queue->x_lock_count_--;
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_SHARED:
        lock_request_queue->is_lock_count_--;
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        lock_request_queue->ix_lock_count_--;
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        lock_request_queue->six_lock_count_--;
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        break;
      default:
        throw std::runtime_error("Invalid lock mode");
    }

    // Notify other threads waiting on the lock
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  ValidateLockRowInput(txn, lock_mode, oid, rid);

  // Get the LockRequestQueue for the row
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  row_lock_map_latch_.unlock();

  // Insert the lock request into the queue
  {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    bool upgrade = false;

    // Traverse the queue to check if the txn already has a lock
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); /** no increment here */) {
      LockRequest *existing_request = it->get();
      if (existing_request->txn_id_ == txn->GetTransactionId()) {
        // granted_ must be true, otherwise the txn cannot issue a second lock request
        assert(existing_request->granted_);

        if (existing_request->lock_mode_ == lock_mode) {
          return true;
        }

        // Abort if another transaction is doing a lock upgrade
        if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
          AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
        }

        // Lock upgrade
        LockMode existing_lock_mode = existing_request->lock_mode_;
        assert(existing_lock_mode == LockMode::SHARED);
        if (lock_mode != LockMode::EXCLUSIVE) {
          AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
        }

        // Mark the queue as being upgraded by this txn
        lock_request_queue->upgrading_ = txn->GetTransactionId();
        upgrade = true;

        // Remove the existing request from the queue
        it = lock_request_queue->request_queue_.erase(it);
        // Update the lock count and the txn's lock set
        switch (existing_lock_mode) {
          case LockMode::SHARED:
            lock_request_queue->s_lock_count_--;
            txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
            break;
          case LockMode::EXCLUSIVE:
            lock_request_queue->x_lock_count_--;
            txn->GetExclusiveRowLockSet()->operator[](oid).erase(rid);
            break;
          default:
            throw std::runtime_error("Invalid lock mode");
        }
      }
      else {
        it++;
      }
    }

    // Create a new LockRequest
    std::unique_ptr<LockRequest> lock_request = std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    if (upgrade) {
      lock_request_queue->request_queue_.push_front(std::move(lock_request));
    }
    else {
      lock_request_queue->request_queue_.push_back(std::move(lock_request));
    }
  }

  // Wait for the lock to be granted
  {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    while (!GrantRowLock(txn, lock_mode, lock_request_queue.get())) {
      lock_request_queue->cv_.wait(lock);
    }
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  ValidateUnlockRowInput(txn, oid, rid);
  
  // Get the LockRequestQueue for the row
  row_lock_map_latch_.lock();
  auto lock_request_queue = row_lock_map_.at(rid);
  row_lock_map_latch_.unlock();

  {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    // Traverse the queue to find the txn' lock request
    bool found_request_in_queue = false;
    auto it = lock_request_queue->request_queue_.begin();
    while (it != lock_request_queue->request_queue_.end()) {
      LockRequest *existing_request = it->get();
      if (existing_request->txn_id_ == txn->GetTransactionId()) {
        found_request_in_queue = true;
        break;
      }
      ++it;
    }
    if (!found_request_in_queue) {
      AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    // Transaction state transition
    LockMode lock_mode = it->get()->lock_mode_;
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
        if (txn->GetState() == TransactionState::GROWING)
          txn->SetState(TransactionState::SHRINKING);
        break;
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          if (txn->GetState() == TransactionState::GROWING)
            txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          if (txn->GetState() == TransactionState::GROWING)
            txn->SetState(TransactionState::SHRINKING);
        }
        break;
      default:
        throw std::runtime_error("Invalid isolation level");
    }

    // Remove the LockRequest from the queue
    lock_request_queue->request_queue_.erase(it);

    // Update the lock counts and the txn's lock set
    switch (lock_mode) {
      case LockMode::SHARED:
        lock_request_queue->s_lock_count_--;
        txn->GetSharedRowLockSet()->operator[](oid).erase(rid);
        break;
      case LockMode::EXCLUSIVE:
        lock_request_queue->x_lock_count_--;
        txn->GetExclusiveRowLockSet()->operator[](oid).erase(rid);
        break;
      default:
        throw std::runtime_error("Invalid lock mode");
    }

    // Notify other threads waiting on the lock
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

auto LockManager::ValidateLockTableInput(Transaction *txn, LockMode lock_mode) -> void {
  // Early return checks
  if (!txn) {
    throw std::runtime_error("LockTable called with null txn");
  }
  TransactionState txn_state = txn->GetState();
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    throw std::runtime_error("LockTable called with aborted or committed txn");
  }

  // Check if the transaction state and isolation level are compatible with the lock mode
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  if (txn_state == TransactionState::GROWING) {
    if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  else if (txn_state == TransactionState::SHRINKING) {
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
    }
    else if (isolation_level == IsolationLevel::READ_COMMITTED) {
      // Only S and IS allowed
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    }
    else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      else {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
}

auto LockManager::ValidateLockRowInput(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> void {
  if (!txn) {
    throw std::runtime_error("LockRow called with null txn");
  }
  TransactionState txn_state = txn->GetState();
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    throw std::runtime_error("LockTable called with aborted or committed txn");
  }

  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    AbortTransaction(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // Check if the transaction state and isolation level are compatible with the lock mode
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  if (txn_state == TransactionState::GROWING) {
    if (isolation_level == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
      AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  else if (txn_state == TransactionState::SHRINKING) {
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
    }
    else if (isolation_level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    }
    else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED) {
        AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      else {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  
  // Check if the transaction holds a lock on the table
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid)) {
      AbortTransaction(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  else if (lock_mode == LockMode::SHARED) {
    if (!txn->IsTableSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) && 
        !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionSharedLocked(oid)) {
      AbortTransaction(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
}

auto LockManager::ValidateUnlockTableInput(Transaction *txn, const table_oid_t &oid) -> void {
  if (!txn) {
    throw std::runtime_error("UnlockTable called with null txn");
  }

  // Ensure that the txn has a lock on the table
  if (!txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedLocked(oid) && 
      !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid)) {
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Ensure that the txn is not holding any row locks on the table
  if (txn->GetSharedRowLockSet()->operator[](oid).size() != 0 || txn->GetExclusiveRowLockSet()->operator[](oid).size() != 0) {
    AbortTransaction(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
}

auto LockManager::ValidateUnlockRowInput(Transaction *txn, const table_oid_t &oid, const RID &rid) -> void {
  if (!txn) {
    throw std::runtime_error("UnlockRow called with null txn");
  }

  // Ensure that the txn has a lock on the row
  if (!txn->IsRowExclusiveLocked(oid, rid) && !txn->IsRowSharedLocked(oid, rid)) {
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
}

auto LockManager::ValidateLockUpgrade(LockMode src_mode, LockMode dest_mode, Transaction *txn) -> void {
  if (src_mode == LockMode::INTENTION_SHARED) {
    if (dest_mode == LockMode::SHARED || dest_mode == LockMode::EXCLUSIVE || 
        dest_mode == LockMode::INTENTION_EXCLUSIVE || dest_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return;
    }
  }
  if (src_mode == LockMode::SHARED) {
    if (dest_mode == LockMode::EXCLUSIVE || dest_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return;
    }
  }
  if (src_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (dest_mode == LockMode::EXCLUSIVE || dest_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return;
    }
  }
  if (src_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (dest_mode == LockMode::EXCLUSIVE) {
      return;
    }
  }

  AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
}

auto LockManager::CheckLockCompatibility(LockMode mode_1, LockMode mode_2) -> bool {
  switch (mode_1) {
    case LockMode::INTENTION_SHARED:
      if (mode_2 == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (mode_2 == LockMode::SHARED || mode_2 == LockMode::SHARED_INTENTION_EXCLUSIVE || mode_2 == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED:
      if (mode_2 == LockMode::INTENTION_EXCLUSIVE || mode_2 == LockMode::SHARED_INTENTION_EXCLUSIVE || mode_2 == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (mode_2 != LockMode::INTENTION_SHARED) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
    default:
      throw std::runtime_error("Invalid lock mode");
  }

  return true;
}

auto LockManager::GrantTableLock(Transaction *txn, LockMode lock_mode, LockRequestQueue *lock_request_queue) -> bool {
  // Check the lock's compatibility with granted locks
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      if (lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (lock_request_queue->s_lock_count_ > 0 || lock_request_queue->six_lock_count_ > 0 || lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    case LockMode::SHARED:
      if (lock_request_queue->ix_lock_count_ > 0 || lock_request_queue->six_lock_count_ > 0 || lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_request_queue->ix_lock_count_ > 0 || lock_request_queue->s_lock_count_ > 0 || lock_request_queue->six_lock_count_ > 0 || 
          lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      if (lock_request_queue->is_lock_count_ > 0 || lock_request_queue->ix_lock_count_ > 0 || lock_request_queue->s_lock_count_ > 0 || 
          lock_request_queue->six_lock_count_ > 0 || lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    default:
      throw std::runtime_error("Invalid lock mode");
  }

  // Check the lock's compatibility with requests before it in the queue
  bool found_request_in_queue = false;
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end()) {
    LockRequest *existing_request = it->get();
    if (existing_request->txn_id_ == txn->GetTransactionId()) {
      found_request_in_queue = true;
      break;
    }
    if (!CheckLockCompatibility(lock_mode, existing_request->lock_mode_)) {
      return false;
    }
    ++it;
  }
  assert(found_request_in_queue);

  LockRequest *lock_request = it->get();
  lock_request->granted_ = true;
  // Update the lock counts and the txn's lock set
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      lock_request_queue->is_lock_count_++;
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_request_queue->ix_lock_count_++;
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED:
      lock_request_queue->s_lock_count_++;
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_request_queue->six_lock_count_++;
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::EXCLUSIVE:
      lock_request_queue->x_lock_count_++;
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    default:
      throw std::runtime_error("Invalid lock mode");
  }

  // If the txn is upgrading, set the upgrading_ of the lock_request_queue to INVALID_TXN_ID.
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  return true;
}

auto LockManager::GrantRowLock(Transaction *txn, LockMode lock_mode, LockRequestQueue *lock_request_queue) -> bool {
  // Check the lock's compatibility with granted locks
  switch (lock_mode) {
    case LockMode::SHARED:
      if (lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      if (lock_request_queue->s_lock_count_ > 0 || lock_request_queue->x_lock_count_ > 0) {
        return false;
      }
      break;
    default:
      throw std::runtime_error("Invalid lock mode");
  }

  // Check the lock's compatibility with requests before it in the queue
  bool found_request_in_queue = false;
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end()) {
    LockRequest *existing_request = it->get();
    if (existing_request->txn_id_ == txn->GetTransactionId()) {
      found_request_in_queue = true;
      break;
    }
    if (!CheckLockCompatibility(lock_mode, existing_request->lock_mode_)) {
      return false;
    }
    ++it;
  }
  assert(found_request_in_queue);

  LockRequest *lock_request = it->get();
  lock_request->granted_ = true;
  // Update the lock counts and the txn's lock set
  switch (lock_mode) {
    case LockMode::SHARED:
      lock_request_queue->s_lock_count_++;
      txn->GetSharedRowLockSet()->operator[](lock_request->oid_).insert(lock_request->rid_);
      break;
    case LockMode::EXCLUSIVE:
      lock_request_queue->x_lock_count_++;
      txn->GetExclusiveRowLockSet()->operator[](lock_request->oid_).insert(lock_request->rid_);
      break;
    default:
      throw std::runtime_error("Invalid lock mode");
  }

  // If the txn is upgrading, set the upgrading_ of the lock_request_queue to INVALID_TXN_ID.
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  return true;
}

}  // namespace bustub
