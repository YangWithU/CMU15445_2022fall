//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);

  auto GetItem(int index) const -> const MappingType &;

  void InitNewRoot(const ValueType &old_val, const KeyType &new_key, const ValueType &new_val);

  // during insert, as parent_page not full, directly insert data
  void InsertDataToParentPage(const ValueType &old_val, const KeyType &new_key, const ValueType &new_val);

  // find value in current Internal page: array_, by using lower_bound
  // returns value
  auto FindValueOnInternalPage(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  // helper method to get value index
  auto GetValueIndex(const ValueType &value) -> int;

  auto MoveHalfTo(BPlusTreeInternalPage *dst_internal_page, BufferPoolManager *bpm) -> void;

  auto MoveAllTo(BPlusTreeInternalPage *dst_leaf_page, const KeyType &middle_key, BufferPoolManager *bpm) -> void;

  auto CopyNToArrBack(const MappingType *start, int size, BufferPoolManager *bpm) -> void;

  auto MoveFirstToEndOf(BPlusTreeInternalPage *dst_intern_page, const KeyType &middle_key, BufferPoolManager *bpm)
      -> void;
  auto MoveLastToFrontOf(BPlusTreeInternalPage *dst_intern_page, const KeyType &middle_key, BufferPoolManager *bpm)
      -> void;

  auto InsertToBack(const MappingType &element, BufferPoolManager *bpm) -> void;
  auto InsertToFront(const MappingType &element, BufferPoolManager *bpm) -> void;

  auto FillEmptyAftCoal(int index) -> void;

 private:
  // Flexible array member for page data.

  // key存当前page_id, value存下一个page_id
  MappingType array_[1];
};
}  // namespace bustub
