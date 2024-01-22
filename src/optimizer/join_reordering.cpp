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
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::HasIndex(AbstractPlanNodeRef plan, const ColumnValueExpression *expr) -> bool {
	if (plan->GetType() != PlanType::SeqScan) {
		return false;
	}
	auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
	auto index = MatchIndex(seq_scan_plan.table_name_, expr->GetColIdx());
	return index.has_value();
}

auto Optimizer::JoinReordering(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(JoinReordering(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

	if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
		return optimized_plan;
	}

	const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
	BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

	auto left_plan = nlj_plan.GetLeftPlan();
	auto right_plan = nlj_plan.GetRightPlan();

	// If either of the child is not a SeqScan or MockScan, just return the original plan.
	if (left_plan->GetType() != PlanType::SeqScan && left_plan->GetType() != PlanType::MockScan) {
		return optimized_plan;
	}
	if (right_plan->GetType() != PlanType::SeqScan && right_plan->GetType() != PlanType::MockScan) {
		return optimized_plan;
	}
	LOG_INFO("Reordering NLJ");
	
	// Check if expr is equal condition
	auto expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate());
	if (expr == nullptr || expr->comp_type_ != ComparisonType::Equal) {
		return optimized_plan;
	}

	auto left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
	auto right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
	if (left_expr == nullptr || right_expr == nullptr) {
		return optimized_plan;
	}

	auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
	auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());

	// If only right table has index, just return the original plan.
	if (HasIndex(right_plan, right_expr) && !HasIndex(left_plan, left_expr)) {
		return optimized_plan;
	}
	// If only left table has index, swap the left and right table.
	if (HasIndex(left_plan, left_expr) && !HasIndex(right_plan, right_expr)) {
		LOG_INFO("Only left table has index, swap the left and right table.");
		return std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(NestedLoopJoinPlanNode::InferJoinSchema(*right_plan, *left_plan)), std::move(right_plan), std::move(left_plan), 
			std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)), nlj_plan.GetJoinType());
	}

	// Choose the smaller table as the left table.
	std::optional<size_t> left_table_size;
	if (left_plan->GetType() == PlanType::SeqScan) {
		auto left_seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*left_plan);
		left_table_size = EstimatedCardinality(left_seq_scan_plan.table_name_);
	}
	else {
		auto left_mock_scan_plan = dynamic_cast<const MockScanPlanNode &>(*left_plan);
		left_table_size = EstimatedCardinality(left_mock_scan_plan.GetTable());
	}
	BUSTUB_ASSERT(left_table_size.has_value(), "left table size should have value");
	
	std::optional<size_t> right_table_size;
	if (right_plan->GetType() == PlanType::SeqScan) {
		auto right_seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*right_plan);
		right_table_size = EstimatedCardinality(right_seq_scan_plan.table_name_);
	}
	else {
		auto right_mock_scan_plan = dynamic_cast<const MockScanPlanNode &>(*right_plan);
		right_table_size = EstimatedCardinality(right_mock_scan_plan.GetTable());
	}
	BUSTUB_ASSERT(right_table_size.has_value(), "right table size should have value");

	if (left_table_size.value() > right_table_size.value()) {
		return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, right_plan, left_plan, 
			std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)), nlj_plan.GetJoinType());
	}

	return optimized_plan;
}

} // namespace bustub