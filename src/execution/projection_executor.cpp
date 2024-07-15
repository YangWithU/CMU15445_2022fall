#include "execution/executors/projection_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

ProjectionExecutor::ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                                       std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void ProjectionExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto ProjectionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  // Get the next tuple
  const auto status = child_executor_->Next(&child_tuple, rid);

  if (!status) {
    return false;
  }

  // Compute expressions
  std::vector<Value> values{};

  // 根据plan_的schema得到需要的结果的column条数， 分配空间
  values.reserve(GetOutputSchema().GetColumnCount());

  // 执行projection操作，根据expr的column_id_从tuple上拿到数据，最后存到values中构造tuple
  for (const auto &expr : plan_->GetExpressions()) {
    values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
  }

  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}
}  // namespace bustub
