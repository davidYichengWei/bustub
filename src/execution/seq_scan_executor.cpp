//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {
  Catalog *catalog = exec_ctx->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
  table_heap_ = table_info->table_.get();
}

void SeqScanExecutor::Init() {
  table_iterator_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_ == table_heap_->End()) {
    return false;
  }

  *tuple = *table_iterator_;
  *rid = tuple->GetRid();
  ++table_iterator_;
  return true;
}

}  // namespace bustub
