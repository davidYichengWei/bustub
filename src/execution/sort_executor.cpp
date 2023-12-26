#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
	child_executor_->Init();
	Tuple tuple;
	RID rid;

	while(child_executor_->Next(&tuple, &rid)) {
		tuples_.push_back(tuple);
	}

	if (tuples_.size() <= 1) {
		return;
	}

	// Get the order_by pairs
	const std::vector<OrderBy> &order_bys = plan_->GetOrderBy();

	// Sort the tuples based on the order_bys
	std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &a, const Tuple &b) {
		return CustomComparator(a, b, order_bys);
	});

	current_tuple_idx_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if (current_tuple_idx_ >= tuples_.size()) {
		return false;
	}

	*tuple = tuples_[current_tuple_idx_++];
	return true;
}

bool SortExecutor::CustomComparator(const Tuple &a, const Tuple &b, const std::vector<OrderBy> &order_bys) {
	for (auto [order_by_type, expr] : order_bys) {
		Value a_value = expr->Evaluate(&a, child_executor_->GetOutputSchema());
		Value b_value = expr->Evaluate(&b, child_executor_->GetOutputSchema());

		if (a_value.CompareEquals(b_value) == CmpBool::CmpTrue) {
			continue;
		}

		if (a_value.CompareLessThan(b_value) == CmpBool::CmpTrue) {
			return order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT;
		}
		else {
			return order_by_type == OrderByType::DESC;
		}
	}

	return false;
}

}  // namespace bustub
