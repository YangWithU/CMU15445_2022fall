#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  auto pq_comparator = [&order_bys = plan_->order_bys_, &schema = child_executor_->GetOutputSchema()](
                           const Tuple &tuple_a, const Tuple &tuple_b) -> bool {
    for (const auto &[orderby_type, orderby_expr] : order_bys) {
      auto val_a = orderby_expr->Evaluate(&tuple_a, schema);
      auto val_b = orderby_expr->Evaluate(&tuple_b, schema);
      switch (orderby_type) {
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
          if (static_cast<bool>(val_a.CompareLessThan(val_b))) {
            return false;
          }
          if (static_cast<bool>(val_a.CompareGreaterThan(val_b))) {
            return true;
          }
          break;
      }
    }
    return false;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(pq_comparator)> pq(pq_comparator);

  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    pq.push(child_tuple);
    // 必须要先push再pop,保证按照次序进行驱逐
    // ASC则驱逐最大,DESC驱逐最小
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    child_tuples_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_tuples_.empty()) {
    return false;
  }

  *tuple = child_tuples_.top();
  *rid = tuple->GetRid();
  child_tuples_.pop();

  return true;
}

}  // namespace bustub
