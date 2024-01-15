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
      // If the txn has been aborted, then return false
      if (txn->GetState() == TransactionState::ABORTED) {
        return false;
      }
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
      // If the txn has been aborted, then return false
      if (txn->GetState() == TransactionState::ABORTED) {
        return false;
      }
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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_latch_;
  // If t1 already waits for t2, then do nothing
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if (*it == t2) {
      return;
    }
  }

  waits_for_[t1].push_back(t2);
  vertex_set_.insert(t1);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> waits_for_latch_;
  if (waits_for_[t1].size() == 0) return;
  // Locate t2 in waits_for_[t1] and remove it
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if (*it == t2) {
      waits_for_[t1].erase(it);
      break;
    }
  }

  if (waits_for_[t1].size() == 0) {
    vertex_set_.erase(t1);
  }
}

auto LockManager::CycleDetectionVisit(txn_id_t v, std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &visiting, std::list<txn_id_t> &current_path) -> void {
  assert(visited.find(v) == visited.end());
  assert(visiting.find(v) == visiting.end());

  visiting.insert(v);
  current_path.push_front(v);

  for (auto u : waits_for_[v]) {
    // If a cycle is found, just return
    if (txn_to_abort_ != INVALID_TXN_ID) {
      return;
    }

    if (visiting.find(u) != visiting.end()) {
      // Found a cycle, record it
      // Traverse current_path to find vertices in the cycle
      // u should be alreasy in current_path
      for (auto it = current_path.begin(); it != current_path.end(); it++) {
        txn_to_abort_ = std::max(txn_to_abort_, *it);
        if (*it == u) {
          break;
        }
      }
      break;
    }
    else if (visited.find(u) == visited.end()) {
      CycleDetectionVisit(u, visited, visiting, current_path);
    }
  }

  visiting.erase(v);
  visited.insert(v);
  current_path.pop_front();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unique_lock<std::mutex> waits_for_latch_;

  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> visiting;
  std::list<txn_id_t> current_path;

  for (txn_id_t v : vertex_set_) {
    if (visited.find(v) == visited.end()) {
      CycleDetectionVisit(v, visited, visiting, current_path);
    }
  }

  if (txn_to_abort_ != INVALID_TXN_ID) {
    *txn_id = txn_to_abort_;
    return true;
  }
  else {
    return false;
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  {
    std::unique_lock<std::mutex> lock(waits_for_latch_);
    for (auto [src_txn_id, txn_id_list] : waits_for_) {
      for (auto dest_txn_id : txn_id_list) {
        edges.push_back(std::make_pair(src_txn_id, dest_txn_id));
      }
    }
  }

  return edges;
}

auto LockManager::DeadlockAbort(txn_id_t txn_id) -> void {
  Transaction *txn = TransactionManager::GetTransaction(txn_id);
  txn->LockTxn();
  txn->SetState(TransactionState::ABORTED);

  // Clear the txn's lock sets
  txn->GetSharedTableLockSet()->clear();
  txn->GetExclusiveTableLockSet()->clear();
  txn->GetIntentionSharedTableLockSet()->clear();
  txn->GetIntentionExclusiveTableLockSet()->clear();
  txn->GetSharedIntentionExclusiveTableLockSet()->clear();
  txn->GetSharedRowLockSet()->clear();
  txn->GetExclusiveRowLockSet()->clear();
  txn->UnlockTxn();

  // Traverse all row lock request queues to release resources and notify waiting threads
  row_lock_map_latch_.lock();
  for (auto [rid, lock_request_queue] : row_lock_map_) {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
      LockRequest *existing_request = it->get();
      if (existing_request->txn_id_ == txn_id) {
        // If the request is granted, then we need to update the lock counts and the txn's lock set
        if (existing_request->granted_) {
          switch (existing_request->lock_mode_) {
            case LockMode::SHARED:
              lock_request_queue->s_lock_count_--;
              break;
            case LockMode::EXCLUSIVE:
              lock_request_queue->x_lock_count_--;
              break;
            default:
              throw std::runtime_error("Invalid lock mode");
          }
        }
        // If the request is upgrading, then we need to set the upgrading_ of the LockRequestQueue to INVALID_TXN_ID
        if (lock_request_queue->upgrading_ == txn_id) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
        }
        lock_request_queue->request_queue_.erase(it);

        // Notify threads waiting on the lock
        lock_request_queue->cv_.notify_all();
        break;
      }
    }
  }
  row_lock_map_latch_.unlock();

  // Traverse all table lock request queues to release resources and notify waiting threads
  table_lock_map_latch_.lock();
  for (auto [oid, lock_request_queue] : table_lock_map_) {
    std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
      LockRequest *existing_request = it->get();
      if (existing_request->txn_id_ == txn_id) {
        // If the request is granted, then we need to update the lock counts and the txn's lock set
        if (existing_request->granted_) {
          switch (existing_request->lock_mode_) {
            case LockMode::SHARED:
              lock_request_queue->s_lock_count_--;
              break;
            case LockMode::EXCLUSIVE:
              lock_request_queue->x_lock_count_--;
              break;
            case LockMode::INTENTION_SHARED:
              lock_request_queue->is_lock_count_--;
              break;
            case LockMode::INTENTION_EXCLUSIVE:
              lock_request_queue->ix_lock_count_--;
              break;
            case LockMode::SHARED_INTENTION_EXCLUSIVE:
              lock_request_queue->six_lock_count_--;
              break;
            default:
              throw std::runtime_error("Invalid lock mode");
          }
        }
        // If the request is upgrading, then we need to set the upgrading_ of the LockRequestQueue to INVALID_TXN_ID
        if (lock_request_queue->upgrading_ == txn_id) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
        }
        lock_request_queue->request_queue_.erase(it);

        // Notify threads waiting on the lock
        lock_request_queue->cv_.notify_all();
        break;
      }
    }
  }
  table_lock_map_latch_.unlock();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);

    BuildWaitsForGraph();
    txn_id_t txn_to_abort = INVALID_TXN_ID;
    while (HasCycle(&txn_to_abort)) {
      // Abort the youngest txn in the cycle
      DeadlockAbort(txn_to_abort);
      // Update the waits-for graph and run cycle detection again
      BuildWaitsForGraph();
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

auto LockManager::BuildWaitsForGraph() -> void {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  std::unique_lock<std::mutex> table_lock_map_lock(table_lock_map_latch_);
  std::unique_lock<std::mutex> row_lock_map_lock(row_lock_map_latch_);
  waits_for_.clear();
  vertex_set_.clear();
  txn_to_abort_ = INVALID_TXN_ID;

  for (auto [oid, request_queue] : table_lock_map_) {
    /**
     * Algorithm:
     * Traverse the queue and build 2 sets with txn ids of granted and waiting txns.
     * For each txn in the waiting set, add an edge from each txn in the granted set to the txn in the waiting set.
     */
    std::unordered_set<txn_id_t> granted_set;
    std::unordered_set<txn_id_t> waiting_set;
    for (auto it = request_queue->request_queue_.begin(); it != request_queue->request_queue_.end(); it++) {
      LockRequest *lock_request = it->get();
      if (lock_request->granted_) {
        granted_set.insert(lock_request->txn_id_);
      }
      else {
        waiting_set.insert(lock_request->txn_id_);
      }
    }

    for (auto waiting_txn_id : waiting_set) {
      for (auto granted_txn_id : granted_set) {
        AddEdge(granted_txn_id, waiting_txn_id);
      }
    }
  }

  for (auto [rid, request_queue] : row_lock_map_) {
    std::unordered_set<txn_id_t> granted_set;
    std::unordered_set<txn_id_t> waiting_set;
    for (auto it = request_queue->request_queue_.begin(); it != request_queue->request_queue_.end(); it++) {
      LockRequest *lock_request = it->get();
      if (lock_request->granted_) {
        granted_set.insert(lock_request->txn_id_);
      }
      else {
        waiting_set.insert(lock_request->txn_id_);
      }
    }

    for (auto waiting_txn_id : waiting_set) {
      for (auto granted_txn_id : granted_set) {
        AddEdge(granted_txn_id, waiting_txn_id);
      }
    }
  }
}

}  // namespace bustub
