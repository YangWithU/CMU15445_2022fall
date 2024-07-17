//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  // 将所有mockscan吐的tuple插入aht_.ht_中，并将所有value初始化为1
  // key采用group_by，value则是count,sum等算子
  // 可以发现，对于同一列的所有tuple,它们的key都相同,同一个group_by vector<Value>
  // 所以初始化的插入过程一直对同一个key的value进行add(1)
  while (child_->Next(&tuple, &rid)) {
    // MakeAggregateValue:对一个tuple遍历所有的aggregates_, **从而获得下一组将要被aggregate的数据**
    // 返回计算结果AggregateValue即vector<Value>
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertInitialCombine();
  }
  aht_iterator_ = aht_.Begin();
}

// 应该计算的aggregate已经存在aht.ht_上了,Next直接迭代器遍历hashtable构造tuple返回就行了
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values{};
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());

  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
