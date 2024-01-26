//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  left_key_expr_ = dynamic_cast<const ColumnValueExpression *>(&plan_->LeftJoinKeyExpression());
  right_key_expr_ = dynamic_cast<const ColumnValueExpression *>(&plan_->RightJoinKeyExpression());
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // Initialize current_left_key_
  left_executor_->Next(&current_left_tuple_, &current_left_rid_);
  if (!current_left_tuple_.IsAllocated()) {
    left_exausted_ = true;
    return;
  }
  current_left_key_ = ExtractHashKey(current_left_tuple_, left_key_expr_, left_executor_->GetOutputSchema());

  // Build hash table for the right table
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    // Extract the join key from the right tuple
    HashKey right_join_hash_key = ExtractHashKey(right_tuple, right_key_expr_, right_executor_->GetOutputSchema());

    // Insert the key and an empty vector of tuples if the key does not exist
    if (hash_table_.find(right_join_hash_key) == hash_table_.end()) {
      hash_table_.insert({right_join_hash_key, std::vector<Tuple>()});
    }

    hash_table_[right_join_hash_key].push_back(right_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!left_exausted_) {
    if (right_tuple_index_ == hash_table_[current_left_key_].size()) {
      Tuple prev_left_tuple = current_left_tuple_;
      if (!left_executor_->Next(&current_left_tuple_, &current_left_rid_)) {
        left_exausted_ = true;
      }
      // Update current_left_key_ and reset right_tuple_index_
      current_left_key_ = ExtractHashKey(current_left_tuple_, left_key_expr_, left_executor_->GetOutputSchema());
      right_tuple_index_ = 0;

      // LEFT JOIN: if no match in current iteration, output prev left tuple with nulls
      if (plan_->GetJoinType() == JoinType::LEFT && !any_match_in_current_left_tuple_) {
        *tuple = MakeOutputTuple(&prev_left_tuple);
        return true;
      }

      any_match_in_current_left_tuple_ = false;
      continue;
    }

    Tuple right_matching_tuple = hash_table_[current_left_key_][right_tuple_index_++];
    any_match_in_current_left_tuple_ = true;
    *tuple = MakeOutputTuple(&current_left_tuple_, &right_matching_tuple);
    return true;
  }

  return false;
}

auto HashJoinExecutor::ExtractHashKey(Tuple tuple, const ColumnValueExpression *expr, const Schema &schema) -> HashKey {
  auto col_idx = expr->GetColIdx();
  Value join_key = tuple.GetValue(&schema, col_idx);
  return HashKey{join_key};
}

auto HashJoinExecutor::MakeOutputTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  std::vector<Value> values;
  Schema left_schema = left_executor_->GetOutputSchema();
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }

  Schema right_schema = right_executor_->GetOutputSchema();
  for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
    // Add null if right_tuple is nullptr
    if (right_tuple == nullptr) {
      values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
    }
    else {
      values.push_back(right_tuple->GetValue(&right_schema, i));
    }
  }

  return Tuple{values, &GetOutputSchema()};
}

}  // namespace bustub
