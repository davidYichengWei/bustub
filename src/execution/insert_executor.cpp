//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
                               : AbstractExecutor(exec_ctx), exec_ctx_(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  Catalog *catalog = exec_ctx->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());

  std::string table_name = catalog->GetTable(plan_->TableOid())->name_;
  index_info_list_ = catalog->GetTableIndexes(table_name);
}

void InsertExecutor::Init() {
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    *tuple = Tuple({Value(INTEGER, insert_count)}, &plan_->OutputSchema());
    return false;
  }

  Tuple *tuple_to_insert = new Tuple();
  while (child_executor_->Next(tuple_to_insert, rid)) {
    table_info_->table_->InsertTuple(*tuple_to_insert, rid, exec_ctx_->GetTransaction());
    insert_count++;

    // Update index
    for (auto index_info : index_info_list_) {
      Tuple key_tuple = tuple_to_insert->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  executed_ = true;

  *tuple = Tuple({Value(INTEGER, insert_count)}, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
