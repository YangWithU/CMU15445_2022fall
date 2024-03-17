/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int idx)
    : buffer_pool_manager_(bpm), p_page_(page), index_(idx) {
  if (page != nullptr) {
    p_leaf_ = reinterpret_cast<LeafPage *>(page->GetData());
  } else {
    p_leaf_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (p_page_ != nullptr) {
    p_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(p_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return index_ == p_leaf_->GetSize() && p_leaf_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return p_leaf_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ == p_leaf_->GetSize() - 1 && p_leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    auto nxt_page = buffer_pool_manager_->FetchPage(p_leaf_->GetNextPageId());

    nxt_page->RLatch();
    p_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(p_page_->GetPageId(), false);

    p_page_ = nxt_page;
    p_leaf_ = reinterpret_cast<LeafPage *>(p_page_->GetData());
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  // why p_leaf_ == nullptr ?
  return p_leaf_ == nullptr || (p_leaf_->GetPageId() == itr.p_leaf_->GetPageId() && index_ == itr.index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
