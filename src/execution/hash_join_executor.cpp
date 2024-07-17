//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple tmp_tuple{};
  RID rid{};

  // 用右表构造hashmap
  while (right_executor_->Next(&tmp_tuple, &rid)) {
    auto rjoin_key = plan_->RightJoinKeyExpression().Evaluate(&tmp_tuple, plan_->GetRightPlan()->OutputSchema());
    hash_join_table_[HashUtil::HashValue(&rjoin_key)].push_back(tmp_tuple);
  }

  while (left_executor_->Next(&tmp_tuple, &rid)) {
    auto ljoin_key = plan_->LeftJoinKeyExpression().Evaluate(&tmp_tuple, plan_->GetLeftPlan()->OutputSchema());
    auto lkey_hash = HashUtil::HashValue(&ljoin_key);

    // 左表上存在之前右表上的记录
    if (hash_join_table_.count(lkey_hash) > 0) {
      auto right_tuples = hash_join_table_[lkey_hash];

      for (const auto &cur_tuple : right_tuples) {
        auto rjoin_key = plan_->RightJoinKeyExpression().Evaluate(&cur_tuple, plan_->GetRightPlan()->OutputSchema());

        if (rjoin_key.CompareEquals(ljoin_key) == CmpBool::CmpTrue) {
          std::vector<Value> values{};
          auto lcol_cnt = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
          auto rcol_cnt = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
          values.reserve(lcol_cnt + rcol_cnt);

          for (uint32_t col_idx = 0; col_idx < lcol_cnt; col_idx++) {
            values.push_back(tmp_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
          }
          for (uint32_t col_idx = 0; col_idx < rcol_cnt; col_idx++) {
            values.push_back(cur_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_idx));
          }

          output_tuples_.emplace_back(values, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      auto lcol_cnt = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
      auto rcol_cnt = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
      values.reserve(lcol_cnt + rcol_cnt);

      for (uint32_t col_idx = 0; col_idx < lcol_cnt; col_idx++) {
        values.push_back(tmp_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < rcol_cnt; col_idx++) {
        values.push_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(col_idx).GetType()));
      }

      output_tuples_.emplace_back(values, &GetOutputSchema());
    }
  }

  output_tuples_iter_ = output_tuples_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_tuples_iter_ == output_tuples_.cend()) {
    return false;
  }

  *tuple = *output_tuples_iter_;
  *rid = tuple->GetRid();

  ++output_tuples_iter_;

  return true;
}

}  // namespace bustub
