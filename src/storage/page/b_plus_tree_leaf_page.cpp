//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetSize(0);
  this->SetNextPageId(INVALID_PAGE_ID);
  this->SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> const MappingType & { return array_[index]; }

// helper method for find
//         (LEAF:)
// [index1] | [index2] | [...]  -> array_
// RETUENS index
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  auto it = std::lower_bound(array_, array_ + this->GetSize(), key,
                             [&comparator](auto &pr, auto &k) { return comparator(pr.first, k) < 0; });
  return std::distance(array_, it);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValueOnLeaf(const KeyType &key, ValueType *value, KeyComparator &comparator)
    -> bool {
  auto idx = GetIndex(key, comparator);
  if (idx == this->GetSize() || comparator(array_[idx].first, key) != 0) {
    return false;
  }
  *value = array_[idx].second;
  return true;
}

// insert key[value] to leaf
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> int {
  auto idx = GetIndex(key, comparator);

  // cur leaf node already full
  if (idx == this->GetSize()) {
    array_[idx] = {key, value};
    IncreaseSize(1);
    return this->GetSize();
  }

  // already exists
  if (comparator(array_[idx].first, key) == 0) {
    return this->GetSize();
  }

  // size not full && non-exist in node, insert
  //          key(new)
  //            |
  // [(1, 2, 3) _  (idx + 1...)]
  std::move_backward(array_ + idx, array_ + this->GetSize(), array_ + this->GetSize() + 1);

  array_[idx] = {key, value};

  IncreaseSize(1);
  return this->GetSize();
}

// caller calls MoveHalfTo to move
// splited caller->array into receiver->array
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *dst_leaf_page) -> void {
  int array_split_begin = GetMinSize();

  // set MoveHalfTo caller array size
  SetSize(array_split_begin);

  // move MoveHalfTo caller array into receiver array
  //                      param: caller's array
  dst_leaf_page->CopyNToArrBack(array_ + array_split_begin, GetMaxSize() - array_split_begin);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *dst_leaf_page) -> void {
  auto fronter_item = GetItem(0);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
  dst_leaf_page->InsertToBack(fronter_item);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *dst_leaf_page) -> void {
  auto last_item = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  dst_leaf_page->InsertToFront(last_item);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *dst_leaf_page) -> void {
  dst_leaf_page->CopyNToArrBack(array_, GetSize());
  dst_leaf_page->SetNextPageId(this->GetNextPageId());  // dst 的next page设成当前page的 next page
  SetSize(0);  // set page_to_coalease size 0, 合并完成，自身大小清空
}

/*
 * insert 'size' element into back of array_
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNToArrBack(const MappingType *start, int size) -> void {
  // param: res begin, res end, dst begin
  //                            param: receiver(dst_leaf_page)'s array
  std::copy(start, start + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertToBack(const MappingType &element) -> void {
  *(array_ + GetSize()) = element;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertToFront(const MappingType &element) -> void {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  *(array_) = element;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveArrayRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  auto idx = GetIndex(key, comparator);

  // key not found
  if (idx == GetSize() || comparator(array_[idx].first, key) != 0) {
    return GetSize();
  }

  // 后面移到前面
  std::move(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
  return GetSize();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
