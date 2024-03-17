//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::PickReplacementFrame(frame_id_t *frame_id) -> bool {
  frame_id_t lookup_frame = -1;
  // first lookup frame managed by buffer pool
  if (!free_list_.empty()) {
    lookup_frame = free_list_.front();
    free_list_.pop_front();
    *frame_id = lookup_frame;
    return true;
  }

  // no free in the free list, evict saved frame from buffer pool
  if (replacer_->Evict(&lookup_frame)) {
    auto evicted_page_id = pages_[lookup_frame].GetPageId();
    if (pages_[lookup_frame].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[lookup_frame].GetData());
      pages_[lookup_frame].is_dirty_ = false;
    }

    page_table_->Remove(evicted_page_id);
    *frame_id = lookup_frame;

    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  // whole pages were used
  bool has_free = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      has_free = true;
      break;
    }
  }
  if (!has_free) {
    return nullptr;
  }

  *page_id = AllocatePage();

  frame_id_t lookup_frame = -1;
  if (!PickReplacementFrame(&lookup_frame)) {
    return nullptr;
  }

  // now frame was available, save data "to" frame
  page_table_->Insert(*page_id, lookup_frame);

  pages_[lookup_frame].ResetMemory();
  pages_[lookup_frame].page_id_ = *page_id;
  pages_[lookup_frame].pin_count_ = 1;

  replacer_->RecordAccess(lookup_frame);
  replacer_->SetEvictable(lookup_frame, false);

  return &pages_[lookup_frame];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t lookup_frame = -1;
  if (page_table_->Find(page_id, lookup_frame)) {
    pages_[lookup_frame].pin_count_++;
    replacer_->RecordAccess(lookup_frame);
    replacer_->SetEvictable(lookup_frame, false);
    return &pages_[lookup_frame];
  }

  /* from now, means page not exist in buffer pool */

  // pinned
  bool has_free = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      has_free = true;
      break;
    }
  }
  if (!has_free) {
    return nullptr;
  }

  if (!PickReplacementFrame(&lookup_frame)) {
    return nullptr;
  }

  // now frame was available, save data "to" frame
  page_table_->Insert(page_id, lookup_frame);

  pages_[lookup_frame].page_id_ = page_id;
  pages_[lookup_frame].pin_count_ = 1;

  // load legacy data
  disk_manager_->ReadPage(page_id, pages_[lookup_frame].GetData());

  replacer_->RecordAccess(lookup_frame);
  replacer_->SetEvictable(lookup_frame, false);

  return &pages_[lookup_frame];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t lookup_frame = -1;
  if (!page_table_->Find(page_id, lookup_frame)) {
    return false;
  }

  if (pages_[lookup_frame].GetPinCount() <= 0) {
    return false;
  }

  pages_[lookup_frame].pin_count_--;

  if (pages_[lookup_frame].pin_count_ == 0) {
    replacer_->SetEvictable(lookup_frame, true);
  }

  if (is_dirty) {
    pages_[lookup_frame].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t lookup_frame = -1;
  if (page_table_->Find(page_id, lookup_frame)) {
    disk_manager_->WritePage(page_id, pages_[lookup_frame].GetData());
    pages_[lookup_frame].is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(i);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t lookup_frame = -1;
  if (!page_table_->Find(page_id, lookup_frame)) {
    return true;
  }

  if (pages_[lookup_frame].GetPinCount() > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(lookup_frame);
  free_list_.push_back(lookup_frame);

  pages_[lookup_frame].ResetMemory();
  pages_[lookup_frame].page_id_ = INVALID_PAGE_ID;
  pages_[lookup_frame].is_dirty_ = false;
  pages_[lookup_frame].pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
