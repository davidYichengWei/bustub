//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
                                     : AbstractExecutor(exec_ctx), exec_ctx_(exec_ctx), plan_(plan) {
  Catalog *catalog = exec_ctx->GetCatalog();

  IndexInfo *index_info = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info->table_name_);

  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
}

void IndexScanExecutor::Init() {
  index_iterator_ = index_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_iterator_.IsEnd()) {
    return false;
  }

  // Get rid from index iterator
  *rid = (*index_iterator_).second;
  // Get tuple from table heap
  table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++index_iterator_;

  return true;
}

}  // namespace bustub
