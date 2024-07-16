//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 * 数据库管理系统（DBMS）在特定情况下使用的一种查询执行计划节点，称为 `NestedIndexJoinPlanNode`。具体来说，这种计划节点在以下条件下会被使用：

 * 1. **查询包含一个连接（join）操作**：这意味着查询涉及两个或多个表之间的连接操作。
 * 2. **连接操作包含一个等值条件（equi-condition）**：等值条件是指连接条件中的列是通过等号（=）进行比较的
 * 例如，`table1.columnA = table2.columnB` 就是一个等值条件
 * 3.
 **连接的右侧表在连接条件上有一个索引（index）**：这意味着在连接操作中，右侧表的列上存在一个索引。索引可以加速数据检索，因为它允许DBMS快速定位*到特定的数据行，而不是进行全表扫描

 * 在这种情况下，DBMS 可能会选择使用 `NestedIndexJoinPlanNode` 来执行查询。这种计划节点通常涉及以下步骤：

 * - **内层索引查找**：对于左侧表的每一行，使用右侧表上的索引快速查找匹配的行
 * - **外层循环**：遍历左侧表的每一行。
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;
  const IndexInfo *index_info_;
  const TableInfo *table_info_;
  BPlusTreeIndexForOneIntegerColumn *bptree_;
};
}  // namespace bustub
