//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Matches(Tuple *left_tuple, Tuple *right_tuple) -> bool {
  auto resval = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                                right_executor_->GetOutputSchema());

  return !resval.IsNull() && resval.GetAs<bool>();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID emit_rid{};
  while (right_tuple_idx_ >= 0 || left_executor_->Next(&left_tuple_, &emit_rid)) {
    std::vector<Value> vals;
    uint32_t r_idx = right_tuple_idx_ < 0 ? 0 : right_tuple_idx_;
    for (; r_idx < right_tuples_.size(); r_idx++) {
      auto &r_tuple = right_tuples_[r_idx];

      // 相匹配，将左右所有row提取出来存到结果tuple
      if (Matches(&left_tuple_, &r_tuple)) {
        for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
          vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col_idx));
        }

        for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
          vals.push_back(r_tuple.GetValue(&right_executor_->GetOutputSchema(), col_idx));
        }

        *tuple = Tuple{vals, &GetOutputSchema()};
        right_tuple_idx_ = r_idx + 1;
        return true;
      }
    }

    // LEFT JOIN 返回左表（即在LEFT JOIN关键字左边的表）中的所有记录
    // 以及右表中满足连接条件的匹配记录
    // 如果右表中没有匹配的记录，则结果集中右表的列将包含NULL值
    if (right_tuple_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(col_idx).GetType()));
      }

      *tuple = Tuple{vals, &GetOutputSchema()};
      return true;
    }

    // reset to default unavailable value
    right_tuple_idx_ = -1;
  }
  return false;
}

}  // namespace bustub
