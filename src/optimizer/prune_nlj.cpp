#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto PredicateAlwaysFalse(AbstractPlanNodeRef plan_node) -> bool {
	if (plan_node->GetType() != PlanType::Filter) {
		return false;
	}

	const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan_node);
	auto predicate = filter_plan.GetPredicate();

	auto comparison_expr = dynamic_cast<const ComparisonExpression *>(predicate.get());
	if (comparison_expr == nullptr) {
		return false;
	}

	if (comparison_expr->comp_type_ != ComparisonType::Equal) {
		return false;
	}
	auto left = comparison_expr->GetChildAt(0);
	auto right = comparison_expr->GetChildAt(1);
	auto left_const_value_expr = dynamic_cast<const ConstantValueExpression *>(left.get());
	auto right_const_value_expr = dynamic_cast<const ConstantValueExpression *>(right.get());
	if (left_const_value_expr == nullptr || right_const_value_expr == nullptr) {
		return false;
	}

	if (left_const_value_expr->val_.CompareEquals(right_const_value_expr->val_) == CmpBool::CmpTrue) {
		return false;
	}

	return true;
}

auto Optimizer::PruneNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
	std::vector<AbstractPlanNodeRef> children;
	for (const auto &child : plan->GetChildren()) {
		children.emplace_back(PruneNLJ(child));
	}
	auto optimized_plan = plan->CloneWithChildren(std::move(children));

	if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
		return optimized_plan;
	}

	auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
	auto left_child = nlj_plan.GetLeftPlan();
	auto right_child = nlj_plan.GetRightPlan();

	if (nlj_plan.GetJoinType() == JoinType::LEFT && PredicateAlwaysFalse(right_child)) {
		return left_child;
	}

	return optimized_plan;
}

}  // namespace bustub
