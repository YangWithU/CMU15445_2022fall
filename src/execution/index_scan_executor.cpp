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
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      bptree_index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())),
      iterator_(plan->filter_predicate_ == nullptr ? bptree_index_->GetBeginIterator()
                                                   : BPlusTreeIndexIteratorForOneIntegerColumn(nullptr, nullptr)) {}

void IndexScanExecutor::Init() {
  if (plan_->filter_predicate_ != nullptr) {
    const auto *right_expr =
        dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Value val = right_expr->val_;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 之前我们在ctor中已经从exec_ctx读取index_info并根据index构造了bptree_index
  // 此时iterator初始化为bptree_index的Begin
  // 所以我们只需遍历bptree的iterator即可
  if (iterator_ == bptree_index_->GetEndIterator()) {
    return false;
  }

  *rid = (*iterator_).second;
  bool res = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());

  ++iterator_;
  return res;
}

}  // namespace bustub
