#include "common/macros.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto const &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::Limit) {
    return optimized_plan;
  }

  const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
  const size_t limit = limit_plan.GetLimit();
  BUSTUB_ENSURE(limit_plan.GetChildren().size() == 1, "Limit should have exactly 1 child.");
  if (limit_plan.GetChildren()[0]->GetType() != PlanType::Sort) {
    return optimized_plan;
  }

  const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*limit_plan.GetChildren()[0]);
  const auto &order_bys = sort_plan.GetOrderBy();
  BUSTUB_ENSURE(sort_plan.GetChildren().size() == 1, "Sort should have exactly 1 child.");

  return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), order_bys, limit);
}

}  // namespace bustub
