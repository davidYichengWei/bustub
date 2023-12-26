//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
	child_executor_->Init();
	Tuple tuple;
	RID rid;

	while (child_executor_->Next(&tuple, &rid)) {
		tuples_.emplace_back(tuple);
	}

	current_tuple_idx_ = 0;
	limit_ = plan_->GetLimit();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if (current_tuple_idx_ == limit_) {
		return false;
	}

	if (current_tuple_idx_ == tuples_.size()) {
		return false;
	}

	*tuple = tuples_[current_tuple_idx_];
	++current_tuple_idx_;

	return true;
}

}  // namespace bustub
