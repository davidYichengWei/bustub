//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
/** HashKey to be used when building the hash table. */
struct HashKey {
  Value hash_key_;

  auto operator==(const HashKey &other) const -> bool {
    if (hash_key_.CompareEquals(other.hash_key_) != CmpBool::CmpTrue) {
      return false;
    }
    return true;
  }
};
} // namespace bustub

namespace std {
/** Implement std::hash on HashKey. */
template <>
struct hash<bustub::HashKey> {
  auto operator()(const bustub::HashKey &key) const -> size_t {
    return bustub::HashUtil::HashValue(&key.hash_key_);
  }
};
}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /**
   * @brief Extract the hash key from the given tuple.
   * 
   * @param tuple 
   * @param expr 
   * @param schema 
   * @return HashKey 
   */
  auto ExtractHashKey(Tuple tuple, const ColumnValueExpression *expr, const Schema &schema) -> HashKey;

  /**
   * @brief Build the output tuple for the JOIN operation.
   * If right_tuple is nullptr, perform a LEFT JOIN by filling the right side with NULL values.
   * 
   * @param left_tuple 
   * @param right_tuple 
   * @return Tuple 
   */
  auto MakeOutputTuple(Tuple *left_tuple, Tuple *right_tuple = nullptr) -> Tuple;

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  [[maybe_unused]] ExecutorContext *exec_ctx_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  // Build hash table for the right table.
  std::unordered_map<HashKey, std::vector<Tuple>> hash_table_;
  [[maybe_unused]] size_t right_tuple_index_ = 0;

  Tuple current_left_tuple_;
  RID current_left_rid_;
  HashKey current_left_key_;
  [[maybe_unused]] bool any_match_in_current_left_tuple_ = false;
  bool left_exausted_ = false;

  // For extracting join key
  const ColumnValueExpression *left_key_expr_;
  const ColumnValueExpression *right_key_expr_;
};

}  // namespace bustub
