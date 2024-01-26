#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/**
 * @brief Collect all comparison expressions in expr recursively.
 *  - Only collect comparison expressions that are in the form of ColumnValueExpression op ConstantValueExpression.
 * 
 * @param[out] comparison_expressions 
 * @param[out] join_comparison_expression capture the comparison expression for join, to be used later to update the predicate 
 * @param expr 
 */
auto CollectComparisonExpressions(std::vector<ComparisonExpression> &comparison_expressions,
                                  ComparisonExpression *&join_comparision_expression,
                                  const AbstractExpression *expr) -> void {
  if (expr == nullptr) {
    return;
  }

  if (const auto *comparison_expression = dynamic_cast<const ComparisonExpression *>(expr); comparison_expression != nullptr) {
    // If both left and right child are ColumnValueExpression, then this is a join predicate
    {
      const auto *left_child = dynamic_cast<const ColumnValueExpression *>(comparison_expression->GetChildAt(0).get());
      const auto *right_child = dynamic_cast<const ColumnValueExpression *>(comparison_expression->GetChildAt(1).get());
      if (left_child != nullptr && right_child != nullptr) {
        join_comparision_expression = new ComparisonExpression(comparison_expression->GetChildAt(0), 
          comparison_expression->GetChildAt(1), comparison_expression->comp_type_);
        return;
      }
    }

    const auto *left_child = dynamic_cast<const ColumnValueExpression *>(comparison_expression->GetChildAt(0).get());
    const auto *right_child = dynamic_cast<const ConstantValueExpression *>(comparison_expression->GetChildAt(1).get());
    if (left_child != nullptr && right_child != nullptr) {
      comparison_expressions.emplace_back(*comparison_expression);
    }
    return;
  }

  if (const auto *logic_expression = dynamic_cast<const LogicExpression *>(expr); logic_expression != nullptr) {
    for (const auto &child : logic_expression->GetChildren()) {
      CollectComparisonExpressions(comparison_expressions, join_comparision_expression, child.get());
    }
    return;
  }
}

/**
 * @brief Merge all comparison expressions into a single LogicExpression.
 * 
 * @param comparision_expressions size >= 2
 * @return std::shared_ptr<LogicExpression> 
 */
auto MergeIntoOneLogicExpression(std::vector<ComparisonExpression> comparision_expressions) -> std::shared_ptr<LogicExpression> {
  BUSTUB_ASSERT(comparision_expressions.size() >= 2, "must be at least 2 comparision expressions to merge");

  // Create two shared_ptr wrappers for the first two comparision expressions
  auto comp_expr_0 = std::make_shared<ComparisonExpression>(comparision_expressions[0]);
  auto comp_expr_1 = std::make_shared<ComparisonExpression>(comparision_expressions[1]);

  auto logic_expression = std::make_shared<LogicExpression>(std::move(comp_expr_0), std::move(comp_expr_1), LogicType::And);
  for (size_t i = 2; i < comparision_expressions.size(); i++) {
    auto comp_expr = std::make_shared<ComparisonExpression>(comparision_expressions[i]);
    logic_expression = std::make_shared<LogicExpression>(std::move(logic_expression), std::move(comp_expr), LogicType::And);
  }
  return logic_expression;
}

/**
 * @brief Merge comparison expressions that have the same left child (operate on the same column) into a single LogicExpression.
 * Leave the comparison expressions that have different left child (operate on different columns) as they are.
 * 
 * @param comparison_expressions 
 * @return std::map<uint32_t, AbstractExpressionRef> key is the col_idx, use pointer to utilize polymorphism.
 */
auto MergeComparisonExpressions(std::vector<ComparisonExpression> &comparison_expressions) -> std::map<uint32_t, AbstractExpressionRef> {
  std::map<uint32_t, AbstractExpressionRef> merged_expressions_map;

  std::map<uint32_t, std::vector<ComparisonExpression>> comparison_expressions_map;
  for (const auto &comparison_expression : comparison_expressions) {
    const auto *left_child = dynamic_cast<const ColumnValueExpression *>(comparison_expression.GetChildAt(0).get());
    comparison_expressions_map[left_child->GetColIdx()].emplace_back(comparison_expression);
  }

  for (const auto &[col_idx, comparison_expressions] : comparison_expressions_map) {
    if (comparison_expressions.size() == 1) {
      merged_expressions_map[col_idx] = std::make_shared<ComparisonExpression>(std::move(comparison_expressions[0]));
    } else {
      merged_expressions_map[col_idx] = MergeIntoOneLogicExpression(comparison_expressions);
    }
  }

  return merged_expressions_map;
}

auto PrintComparisonExpressions(const std::vector<ComparisonExpression> &comparison_expressions) -> void {
  LOG_DEBUG("Printing comparison expressions:");
  for (const auto &comparison_expression : comparison_expressions) {
    LOG_DEBUG("Comparison expression: %s", comparison_expression.ToString().c_str());
  }
}

auto PrintAbstractExpressions(const std::map<uint32_t, AbstractExpressionRef> &abstract_expressions_map) -> void {
  LOG_DEBUG("Printing abstract expressions:");
  for (const auto & [col_idx, abstract_expression] : abstract_expressions_map) {
    // Cast to LogicExpression or ComparisonExpression for printing
    if (const auto *logic_expression = dynamic_cast<const LogicExpression *>(abstract_expression.get()); logic_expression != nullptr) {
      LOG_DEBUG("Logic expression: %s", logic_expression->ToString().c_str());
    } else if (const auto *comparison_expression = dynamic_cast<const ComparisonExpression *>(abstract_expression.get()); comparison_expression != nullptr) {
      LOG_DEBUG("Comparison expression: %s", comparison_expression->ToString().c_str());
    } else {
      LOG_ERROR("Neither logic expression nor comparison expression.");
    }
  }
}

/**
 * @brief Update all column index in the left side of predicates to col_idx.
 * 
 * @param expr 
 * @param col_idx 
 */
auto UpdatePredicateColIdx(AbstractExpressionRef expr, uint32_t col_idx) -> void {
  if (const auto *logic_expression = dynamic_cast<const LogicExpression *>(expr.get()); logic_expression != nullptr) {
    for (const auto &child : logic_expression->GetChildren()) {
      UpdatePredicateColIdx(child, col_idx);
    }
  } else if (const auto *comparison_expression = dynamic_cast<const ComparisonExpression *>(expr.get()); comparison_expression != nullptr) {
    const auto *left_child = dynamic_cast<const ColumnValueExpression *>(comparison_expression->GetChildAt(0).get());
    // Make sure left_child is not nullptr
    BUSTUB_ASSERT(left_child != nullptr, "left_child should not be nullptr");
    left_child->SetColIdx(col_idx);
  } else {
    LOG_ERROR("Neither logic expression nor comparison expression.");
  }
}

auto Optimizer::PredicatePushdown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(PredicatePushdown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::Filter) {
    return optimized_plan;
  }

  auto &filter_plan = dynamic_cast<FilterPlanNode &>(*optimized_plan);
  const auto &child_plan = filter_plan.GetChildPlan();

  auto filter_predicate_expr = filter_plan.GetPredicate();
  // Recursively collect all comparison expressions in filter_predicate
  std::vector<ComparisonExpression> comparison_expressions;

  ComparisonExpression *join_comparison_expression = nullptr;
  CollectComparisonExpressions(comparison_expressions, join_comparison_expression, filter_predicate_expr.get());
  // PrintComparisonExpressions(comparison_expressions);

  // Update filter_predicate_expr to remove predicates that will be pushed down
  if (comparison_expressions.size() > 0) {
    std::shared_ptr<ComparisonExpression> new_filter_predicate_expr(join_comparison_expression);
    filter_plan.SetPredicate(std::move(new_filter_predicate_expr));
  }

  std::map<uint32_t, AbstractExpressionRef> merged_expressions_map = MergeComparisonExpressions(comparison_expressions);
  // PrintAbstractExpressions(merged_expressions_map);

  // Start traversing down the plan tree to find the SeqScan or MockScan node
  for (auto [col_idx, predicate_expr] : merged_expressions_map) {
    const AbstractPlanNode *current_plan_node = optimized_plan.get();
    while (current_plan_node != nullptr) {
      if (const auto *nlj_plan_node = dynamic_cast<const NestedLoopJoinPlanNode *>(current_plan_node); nlj_plan_node != nullptr) {
        // Pick the correct child plan to push down the predicate
        auto left_plan = nlj_plan_node->GetLeftPlan();
        auto right_plan = nlj_plan_node->GetRightPlan();
        auto left_column_count = left_plan->OutputSchema().GetColumnCount();

        if (col_idx < left_column_count) {
          // If the left child is a SeqScan or MockScan, add a FilterPlanNode on top of it
          if (left_plan->GetType() == PlanType::SeqScan || left_plan->GetType() == PlanType::MockScan) {
            // Create a FilterPlanNode with predicate_expr and left_plan as child
            SchemaRef left_child_schema = std::make_shared<Schema>(left_plan->OutputSchema());
            auto filter_plan_node = std::make_shared<FilterPlanNode>(left_child_schema, predicate_expr, std::move(left_plan));

            // Set filter_plan_node as the left child of nlj_plan_node
            nlj_plan_node->SetLeftPlan(std::move(filter_plan_node));

            break;
          }
          else {
            current_plan_node = left_plan.get();
          }
        }
        else {
          // Update col_idx in predicate_expr
          uint32_t new_col_idx = col_idx - left_column_count;
          UpdatePredicateColIdx(predicate_expr, new_col_idx);

          // If the right child is a SeqScan or MockScan, add a FilterPlanNode on top of it
          if (right_plan->GetType() == PlanType::SeqScan || right_plan->GetType() == PlanType::MockScan) {
            SchemaRef right_child_schema = std::make_shared<Schema>(right_plan->OutputSchema());
            auto filter_plan_node = std::make_shared<FilterPlanNode>(right_child_schema, predicate_expr, std::move(right_plan));

            nlj_plan_node->SetRightPlan(std::move(filter_plan_node));

            break;
          }
          else {
            current_plan_node = right_plan.get();
          }
        }
      }
      // TODO: handle the case where a Filter is already present above a scan node
      else {
        // make sure current node only has one child
        BUSTUB_ASSERT(current_plan_node->GetChildren().size() == 1, "current node should have exactly one child");
        current_plan_node = current_plan_node->GetChildAt(0).get();
      }
    }
  }

  return optimized_plan;
}

} // namespace bustub

