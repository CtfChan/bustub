//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

#include "execution/executor_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  // not-raw inserts take values to be inserted from child executor
  if (plan_->IsRawInsert() == false) {
    child_executor_ = ExecutorFactory::CreateExecutor(GetExecutorContext(), plan_->GetChildPlan());
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) { 
  // either raw or not-raw
  if (plan_->IsRawInsert() == true) {
    size_t size = plan_->RawValues().size();
    for (size_t i = 0; i < size; ++i) {
      Tuple tup(plan_->RawValuesAt(i), &table_metadata_->schema_);
      RID rid;
      if (table_metadata_->table_->InsertTuple(tup, &rid, exec_ctx_->GetTransaction()) == false ) 
        return false;
    }
  } else {
    Tuple tup;
    RID rid;
    while (child_executor_->Next(&tup)) {
      if (table_metadata_->table_->InsertTuple(tup, &rid, exec_ctx_->GetTransaction()) == false ) 
        return false;
    }
  }

  return true;   
}

}  // namespace bustub
