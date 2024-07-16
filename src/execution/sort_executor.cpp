#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }

  auto tuple_comparator = [&order_bys = plan_->order_bys_, &schema = child_executor_->GetOutputSchema()](
                              const Tuple &tuple_a, const Tuple &tuple_b) -> bool {
    for (const auto &order_pr : order_bys) {
      auto val_a = order_pr.second->Evaluate(&tuple_a, schema);
      auto val_b = order_pr.second->Evaluate(&tuple_b, schema);
      switch (order_pr.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(val_a.CompareLessThan(val_b))) {
            return true;
          }
          if (static_cast<bool>(val_a.CompareGreaterThan(val_b))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(val_a.CompareGreaterThan(val_b))) {
            return true;
          }
          if (static_cast<bool>(val_a.CompareLessThan(val_b))) {
            return false;
          }
          break;
      }
    }
    return false;
  };

  std::sort(child_tuples_.begin(), child_tuples_.end(), tuple_comparator);

  child_iterator_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
