//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // Initialize current_left_tuple
  left_executor_->Next(&current_left_tuple_, &current_left_rid_);
  if (!current_left_tuple_.IsAllocated()) {
    left_exausted_ = true;
  }

  // Store right tuples if left is not empty
  if (!left_exausted_) {
    Tuple right_tuple;
    RID right_rid;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      right_tuples_.push_back(right_tuple);
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!left_exausted_) {
    if (right_tuple_idx_ == right_tuples_.size()) {
      Tuple prev_left_tuple = current_left_tuple_;
      if (!left_executor_->Next(&current_left_tuple_, &current_left_rid_)) {
        left_exausted_ = true;
      }

      // LEFT JOIN: if no match in current iteration, output prev left tuple with nulls
      if (plan_->GetJoinType() == JoinType::LEFT && !any_match_in_current_left_tuple_) {
        // std::cout << "LEFT JOIN: no match in current iteration, output left tuple with nulls" << std::endl;
        *tuple = MakeOutputTuple(&prev_left_tuple);
        right_tuple_idx_ = 0;

        return true;
      }

      // Reset right after advancing left
      right_tuple_idx_ = 0;
      any_match_in_current_left_tuple_ = false;
      continue;
    }

    Tuple right_tuple = right_tuples_[right_tuple_idx_++];
    if (TuplesMatch(&current_left_tuple_, &right_tuple)) {
      any_match_in_current_left_tuple_ = true;
      *tuple = MakeOutputTuple(&current_left_tuple_, &right_tuple);
      return true;
    }
  }

  return false;
}

auto NestedLoopJoinExecutor::TuplesMatch(Tuple *left_tuple, Tuple *right_tuple) -> bool {
  auto result = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple, right_executor_->GetOutputSchema());
  return !result.IsNull() && result.GetAs<bool>();
}

auto NestedLoopJoinExecutor::MakeOutputTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
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
