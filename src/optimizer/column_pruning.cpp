#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

// 2 possibilities: column value or arithmetic expression
auto GetRequiredColumnIndexes(AbstractExpressionRef projection_expression, std::set<uint32_t> &required_column_indexes) -> void {
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(projection_expression.get()); column_value_expr != nullptr) {
    required_column_indexes.insert(column_value_expr->GetColIdx());
  } 
  else if(const auto *arithmetic_expr = dynamic_cast<const ArithmeticExpression *>(projection_expression.get()); arithmetic_expr != nullptr) {
    for (const auto &child : arithmetic_expr->GetChildren()) {
      GetRequiredColumnIndexes(child, required_column_indexes);
    }
  }
  else {
    throw Exception("Unsupported expression type");
  }
}

auto Optimizer::ColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // ColumnPrune child later

  if (plan->GetType() != PlanType::Projection) {
    std::vector<AbstractPlanNodeRef> children;
    for (const auto &child : plan->GetChildren()) {
      children.emplace_back(ColumnPruning(child));
    }
    auto optimized_plan = plan->CloneWithChildren(std::move(children));
    return optimized_plan;
  }

  const auto *projection_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
  std::set<uint32_t> required_column_indexes;
  for (auto projection_expr : projection_plan->GetExpressions()) {
    GetRequiredColumnIndexes(projection_expr, required_column_indexes);
  }

  auto child = projection_plan->GetChildPlan();
  
  if (child->GetType() == PlanType::Projection) {
    const auto *child_projection_plan = dynamic_cast<const ProjectionPlanNode *>(child.get());
    // Only keep the required columns
    std::vector<AbstractExpressionRef> new_projection_exprs;
    for (uint32_t i = 0; i < child_projection_plan->GetExpressions().size(); i++) {
      if (required_column_indexes.find(i) != required_column_indexes.end()) {
        new_projection_exprs.push_back(child_projection_plan->GetExpressions()[i]);
      }
    }
    child_projection_plan->SetExpressions(new_projection_exprs);

    return ColumnPruning(child);
  }

  if (child->GetType() == PlanType::Aggregation) {
    const auto *child_aggregation_plan = dynamic_cast<const AggregationPlanNode *>(child.get());
    // Only keep the required aggregates and aggregation types
    std::vector<AbstractExpressionRef> required_aggregates;
    for (uint32_t i = 0; i < child_aggregation_plan->GetAggregates().size(); i++) {
      if (required_column_indexes.find(i) != required_column_indexes.end()) {
        required_aggregates.push_back(child_aggregation_plan->GetAggregates()[i]);
      }
    }
    child_aggregation_plan->SetAggregates(required_aggregates);

    std::vector<AggregationType> required_agg_types;
    for (uint32_t i = 0; i < child_aggregation_plan->GetAggregateTypes().size(); i++) {
      if (required_column_indexes.find(i) != required_column_indexes.end()) {
        required_agg_types.push_back(child_aggregation_plan->GetAggregateTypes()[i]);
      }
    }
    child_aggregation_plan->SetAggregateTypes(required_agg_types);
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(ColumnPruning(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}

} // namespace bustub