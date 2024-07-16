//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_info_(this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      bptree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple{};
  RID emit_rid{};
  std::vector<Value> vals;
  while (child_executor_->Next(&left_tuple, &emit_rid)) {
    Value value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());

    std::vector<RID> rids{};
    Tuple key{std::vector<Value>{value}, index_info_->index_->GetKeySchema()};
    bptree_->ScanKey(key, &rids, exec_ctx_->GetTransaction());

    Tuple right_tuple{};
    if (!rids.empty()) {
      // 根据b+树上存的rid去整个table的table_info_上拿对应tuple，无视[0]下标
      table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());

      for (uint32_t col_idx = 0; col_idx < child_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < plan_->InnerTableSchema().GetColumnCount(); col_idx++) {
        vals.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), col_idx));
      }

      *tuple = Tuple{vals, &GetOutputSchema()};
      return true;
    }

    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t col_idx = 0; col_idx < child_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < plan_->InnerTableSchema().GetColumnCount(); col_idx++) {
        vals.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(col_idx).GetType()));
      }

      *tuple = Tuple{vals, &GetOutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
