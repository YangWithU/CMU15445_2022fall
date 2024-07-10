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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void DeleteExecutor::Init() {
  this->child_executor_->Init();
  this->table_indexes_ = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple tp_to_delete{};
  RID emit_rid{};
  int32_t delete_cnt = 0;

  while (this->child_executor_->Next(&tp_to_delete, &emit_rid)) {
    bool deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());

    if (deleted) {
      // table_info_, exec_ctx_: 拷不拷贝？
      auto delete_entry = [&](IndexInfo *indexinfo) -> void {
        indexinfo->index_->DeleteEntry(
            tp_to_delete.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),
            *rid, exec_ctx_->GetTransaction());
      };

      std::for_each(table_indexes_.begin(), table_indexes_.end(), delete_entry);
      delete_cnt++;
    }
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_cnt);

  *tuple = Tuple(values, &GetOutputSchema());
  is_end_ = true;

  return true;
}
}  // namespace bustub
