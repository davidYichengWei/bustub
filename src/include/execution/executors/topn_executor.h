//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <queue>
#include <stack>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

using OrderBy = std::pair<OrderByType, AbstractExpressionRef>;

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /**
   * A custom comparator for the priority queue.
   * Reverse the order as we are using priority queue for topn.
   */
  struct PriorityQueueComparator {
    PriorityQueueComparator(TopNExecutor &executor) : executor_(executor) {}

    /**
     * @brief 
     * 
     * @param a 
     * @param b 
     * @return true if b has a higher priority than a, i.e. closer to the top. (!!! counterintuitive !!!)
     * @return false otherwise.
     */
    bool operator()(const Tuple &a, const Tuple &b);

    TopNExecutor &executor_;
  };

  friend struct PriorityQueueComparator;

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;

  // Using member initializer list to initialize the priority queue with the custom comparator.
  std::priority_queue<Tuple, std::vector<Tuple>, PriorityQueueComparator> tuple_queue_{PriorityQueueComparator(*this)};
  std::stack<Tuple> tuple_stack_; // Sorted tuples, use stack to reverse the order from the priority queue.

  size_t limit_;
};
}  // namespace bustub
