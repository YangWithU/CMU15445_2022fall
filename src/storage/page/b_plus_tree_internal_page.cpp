//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InitNewRoot(const ValueType &old_val, const KeyType &new_key,
                                                 const ValueType &new_val) {
  this->SetKeyAt(1, new_key);
  this->SetValueAt(0, old_val);
  this->SetValueAt(1, new_val);
  this->SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertDataToParentPage(const ValueType &old_val, const KeyType &new_key,
                                                            const ValueType &new_val) {
  auto idx = GetValueIndex(old_val) + 1;
  std::move_backward(array_ + idx, array_ + GetSize(), array_ + GetSize() + 1);

  array_[idx] = {new_key, new_val};

  IncreaseSize(1);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetItem(int index) const -> const MappingType & { return array_[index]; }

// find value in current Internal page: array_, by using lower_bound
// if exact value not found, return prev value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValueOnInternalPage(const KeyType &key, const KeyComparator &comparator) const
    -> ValueType {
  // 记得ignore第一个key
  auto it = std::lower_bound(array_ + 1, array_ + this->GetSize(), key,
                             [&comparator](auto &pr, auto k) { return comparator(pr.first, k) < 0; });

  // not found, return rightus value on node
  if (it == array_ + this->GetSize()) {
    return this->ValueAt(this->GetSize() - 1);
  }

  if (comparator(it->first, key) == 0) {
    return it->second;
  }

  // exact value not found, return prev value
  return std::prev(it)->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueIndex(const ValueType &value) -> int {
  auto idx = std::find_if(array_, array_ + GetSize(), [&](const MappingType &pr) { return pr.second == value; });
  return std::distance(array_, idx);
}

// move array_ from src to dst
// caller: src; param: dst
// internal_page 都在bpm上存在对应
// 故 internal_page 的 MoveHalfTo 涉及到 bpm 的unpin操作
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *dst_internal_page, BufferPoolManager *bpm)
    -> void {
  int array_split_begin = GetMinSize();
  int src_size = GetSize();

  // set old caller(src) size to splitted size
  SetSize(array_split_begin);

  //                               param:src's array_
  // dst call copy func, param: src's stuff
  dst_internal_page->CopyNToArrBack(array_ + array_split_begin, src_size - array_split_begin, bpm);
}

// 操作src page 的函数
// 1. 将旧半数数据拷贝到新internal_page
// 2. 修改原半数数据的子page的parernt_page
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNToArrBack(const MappingType *start, int size, BufferPoolManager *bpm) {
  // 1,2 range: bg, end; 3.dst
  // array_ + GetSize():在原有数据后面copy过来新数据
  std::copy(start, start + size, array_ + GetSize());

  // 更新所有子page
  for (int i = 0; i < size; i++) {
    // copy part: i + dst->Getsize
    // data which we should clean
    auto *page = bpm->FetchPage(ValueAt(i + GetSize()));
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // 原半数数据的子 page 的 parernt_page 改成新 internal_page
    //                        internal_page->GetPageId()
    tree_page->SetParentPageId(this->GetPageId());
    bpm->UnpinPage(page->GetPageId(), true);
  }
  // increase dst size
  IncreaseSize(size);
}

/*
 * Move first element from src to dst_intern_page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *dst_intern_page, const KeyType &middle_key,
                                                      BufferPoolManager *bpm) {
  SetKeyAt(0, middle_key);
  auto fronter_item = GetItem(0);
  dst_intern_page->InsertToBack(fronter_item, bpm);

  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}

// internal_page 处理时需要重设父子节点关系
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToBack(const MappingType &element, BufferPoolManager *bpm) {
  *(array_ + GetSize()) = element;
  IncreaseSize(1);

  auto *page = bpm->FetchPage(element.second);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  tree_page->SetParentPageId(GetPageId());
  bpm->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *dst_intern_page,
                                                       const KeyType &middle_key, BufferPoolManager *bpm) {
  auto last_item = GetItem(GetSize() - 1);
  dst_intern_page->SetKeyAt(0, middle_key);
  dst_intern_page->InsertToFront(last_item, bpm);

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToFront(const MappingType &element, BufferPoolManager *bpm) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  *array_ = element;
  IncreaseSize(1);

  auto *page = bpm->FetchPage(element.second);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  tree_page->SetParentPageId(this->GetPageId());
  bpm->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *dst_leaf_page, const KeyType &middle_key,
                                               BufferPoolManager *bpm) -> void {
  this->SetKeyAt(0, middle_key);
  dst_leaf_page->CopyNToArrBack(this->array_, this->GetSize(), bpm);  // dst 的next page设成当前page的 next page
  SetSize(0);  // set page_to_coalease size 0, 合并完成，自身大小清空
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FillEmptyAftCoal(int index) -> void {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
