//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_metadata_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple) { 
  auto end_iter = table_metadata_->table_->End();
  while (*table_iter_ != end_iter) {
    *tuple = **table_iter_;
    ++(*table_iter_);
    if (!plan_->GetPredicate() || plan_->GetPredicate()->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) 
      return true;
  }
  return false;  
}

}  // namespace bustub
