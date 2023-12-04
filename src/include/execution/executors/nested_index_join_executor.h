//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table (left table, without index)
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /**
   * @brief Get all matching tuples in the inner table based on the given key in current_left_tuple_ using the index.
   * This function also resets right_tuple_idx_ to 0.
   * 
   * @return std::vector<Tuple> 
   */
  std::vector<Tuple> GetInnerMatchingTuples();

  /**
   * @brief Build the output tuple for the JOIN operation.
   * If right_tuple is nullptr, perform a LEFT JOIN by filling the right side with NULL values.
   * 
   * @param left_tuple 
   * @param right_tuple 
   * @return Tuple 
   */
  auto MakeOutputTuple(Tuple *left_tuple, Tuple *right_tuple = nullptr) -> Tuple;

  ExecutorContext *exec_ctx_;
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  BPlusTreeIndexForOneIntegerColumn *index_;
  TableHeap *inner_table_heap_;

  Tuple current_left_tuple_;
  RID current_left_rid_;
  std::vector<Tuple> right_tuples_;
  size_t right_tuple_idx_ = 0;
  bool left_exausted_ = false;
};
}  // namespace bustub
