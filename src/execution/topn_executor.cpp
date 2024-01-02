#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

bool TopNExecutor::PriorityQueueComparator::operator()(const Tuple &a, const Tuple &b) {
	const std::vector<OrderBy> &order_bys = executor_.plan_->GetOrderBy();

	for (auto [order_by_type, expr] : order_bys) {
		Value a_value = expr->Evaluate(&a, executor_.child_executor_->GetOutputSchema());
		Value b_value = expr->Evaluate(&b, executor_.child_executor_->GetOutputSchema());

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

void TopNExecutor::Init() {
	child_executor_->Init();
	const std::vector<OrderBy> order_bys = plan_->GetOrderBy();
	Tuple tuple;
	RID rid;

	limit_ = plan_->GetN();

	while (child_executor_->Next(&tuple, &rid)) {
		// Add tuple into the priority queue
		tuple_queue_.push(tuple);

		// Pop one tuple if the size of the priority queue is greater than the limit
		if (tuple_queue_.size() > limit_) {
			tuple_queue_.pop();
		}
	}

	// Dump the priority queue into the stack
	while (!tuple_queue_.empty()) {
		tuple_stack_.push(tuple_queue_.top());
		tuple_queue_.pop();
	}
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if (tuple_stack_.empty()) {
		return false;
	}

	*tuple = tuple_stack_.top();
	tuple_stack_.pop();

	return true;
}

}  // namespace bustub
