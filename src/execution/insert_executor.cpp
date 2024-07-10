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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple tp_to_insert{};
  RID emit_rid;
  int32_t insert_count = 0;

  while (child_executor_->Next(&tp_to_insert, &emit_rid)) {
    bool inserted = table_info_->table_->InsertTuple(tp_to_insert, &emit_rid, exec_ctx_->GetTransaction());

    if (inserted) {
      auto insert_entry = [&](IndexInfo *idx) {
        idx->index_->InsertEntry(
            tp_to_insert.KeyFromTuple(table_info_->schema_, idx->key_schema_, idx->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
      };

      std::for_each(table_indexes_.begin(), table_indexes_.end(), insert_entry);

      insert_count++;
    }
  }

  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  values.emplace_back(TypeId::INTEGER, insert_count);

  *tuple = Tuple{values, &this->GetOutputSchema()};

  is_end_ = true;
  return true;
}

}  // namespace bustub
