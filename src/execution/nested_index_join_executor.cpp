//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), exec_ctx_(exec_ctx), plan_(plan), left_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  Catalog *catalog = exec_ctx->GetCatalog();
  IndexInfo *index_info = catalog->GetIndex(plan_->GetIndexOid());
  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());

  // Get the table info of the inner table (the table with index)
  TableInfo *inner_table_info = catalog->GetTable(plan_->GetInnerTableOid());
  inner_table_heap_ = inner_table_info->table_.get();
}

void NestIndexJoinExecutor::Init() {
  left_executor_->Init();

  left_executor_->Next(&current_left_tuple_, &current_left_rid_);
  if (!current_left_tuple_.IsAllocated()) {
    left_exausted_ = true;
  }

  // Store right tuples if left is not empty
  if (!left_exausted_) {
    right_tuples_ = GetInnerMatchingTuples();
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!left_exausted_) {
    if (right_tuple_idx_ == right_tuples_.size()) {
      Tuple prev_left_tuple = current_left_tuple_;
      if (!left_executor_->Next(&current_left_tuple_, &current_left_rid_)) {
        left_exausted_ = true;
      }

      // LEFT JOIN: if no match in current iteration, output prev left tuple with nulls
      if (plan_->GetJoinType() == JoinType::LEFT && right_tuples_.size() == 0) {
        *tuple = MakeOutputTuple(&prev_left_tuple);
        right_tuples_ = GetInnerMatchingTuples();

        return true;
      }

      right_tuples_ = GetInnerMatchingTuples();
      continue;
    }

    Tuple right_matching_tuple = right_tuples_[right_tuple_idx_++];
    *tuple = MakeOutputTuple(&current_left_tuple_, &right_matching_tuple);
    return true; 
  }

  return false;
}

std::vector<Tuple> NestIndexJoinExecutor::GetInnerMatchingTuples() {
  if (left_exausted_ || !current_left_tuple_.IsAllocated()) {
    return {};
  }

  std::vector<Tuple> matching_tuples;
  
  // Build a tuple containing only the join key
  auto *col_expr = dynamic_cast<ColumnValueExpression *>(plan_->KeyPredicate().get());
  auto col_idx = col_expr->GetColIdx();

  Schema inner_table_schema = plan_->InnerTableSchema();
  Schema key_schema = Schema::CopySchema(&inner_table_schema, {col_idx});
  Schema left_table_schema = left_executor_->GetOutputSchema();
  Tuple key_probe = current_left_tuple_.KeyFromTuple(left_table_schema, key_schema, {col_idx});

  std::vector<RID> matching_tuple_rids;
  index_->ScanKey(key_probe, &matching_tuple_rids, exec_ctx_->GetTransaction());

  for (auto rid : matching_tuple_rids) {
    Tuple matching_tuple(rid);
    inner_table_heap_->GetTuple(rid, &matching_tuple, exec_ctx_->GetTransaction());
    matching_tuples.push_back(matching_tuple);
  }

  right_tuple_idx_ = 0;
  return matching_tuples;
}

auto NestIndexJoinExecutor::MakeOutputTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  std::vector<Value> values;
  Schema left_schema = left_executor_->GetOutputSchema();
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple->GetValue(&left_schema, i));
  }

  Schema right_schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_;
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
