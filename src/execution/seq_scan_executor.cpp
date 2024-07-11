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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->tableinfo_ = exec_ctx_->GetCatalog()->GetTable(plan->table_oid_);
}

void SeqScanExecutor::Init() { this->table_iter_ = tableinfo_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  do {
    if (table_iter_ == tableinfo_->table_->End()) {
      return false;
    }
    // update: tuple & rid

    // table_iter overloads *operator, returns Table
    auto &table = *table_iter_;
    *tuple = table;

    *rid = tuple->GetRid();

    ++table_iter_;
  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, tableinfo_->schema_).GetAs<bool>());

  return true;
}

}  // namespace bustub
