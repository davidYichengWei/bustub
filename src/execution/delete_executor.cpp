//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
                               : AbstractExecutor(exec_ctx), exec_ctx_(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Catalog *catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());

  std::string table_name = catalog->GetTable(plan_->TableOid())->name_;
  index_info_list_ = catalog->GetTableIndexes(table_name);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    *tuple = Tuple({Value(INTEGER, delete_count_)}, &plan_->OutputSchema());
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    delete_count_++;

    // Update index
    for (auto index_info : index_info_list_) {
      Tuple key_tuple = tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  executed_ = true;

  *tuple = Tuple({Value(INTEGER, delete_count_)}, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
