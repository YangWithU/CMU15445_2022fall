//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // trverse history first, then cache
  bool found_victim = false;
  // since we insert from earlist to oldest, we trverse backwards
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); it++) {
    if (data_[*it].evictable_) {
      *frame_id = *it;
      std::advance(it, 1);
      // remove a reverse iterator
      history_list_.erase(it.base());
      found_victim = true;
      break;
    }
  }

  if (!found_victim && !cache_list_.empty()) {
    for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); it++) {
      if (data_[*it].evictable_) {
        *frame_id = *it;
        std::advance(it, 1);
        cache_list_.erase(it.base());
        found_victim = true;
        break;
      }
    }
  }

  if (found_victim) {
    data_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < (int)replacer_size_, "frame id is invalid: frame_id larger than replacer_size_");

  auto new_cnt = ++data_[frame_id].use_count_;
  if (new_cnt == 1) {  // new data
    history_list_.emplace_front(frame_id);
    data_[frame_id].pos_ = history_list_.begin();
  } else if (new_cnt == k_) {
    // equals to k, remove from history, save to cache
    history_list_.erase(data_[frame_id].pos_);
    cache_list_.emplace_front(frame_id);
    data_[frame_id].pos_ = cache_list_.begin();
  } else if (new_cnt > k_) {
    // so popular, move to cache front
    cache_list_.erase(data_[frame_id].pos_);
    cache_list_.emplace_front(frame_id);
    data_[frame_id].pos_ = cache_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < (int)replacer_size_, "frame id is invalid: frame_id larger than replacer_size_");

  if (data_.find(frame_id) == data_.end()) {
    return;
  }

  if (!data_[frame_id].evictable_ && set_evictable) {
    curr_size_++;
  }
  if (data_[frame_id].evictable_ && !set_evictable) {
    curr_size_--;
  }

  data_[frame_id].evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (data_.find(frame_id) == data_.end()) {
    return;
  }

  BUSTUB_ASSERT(data_[frame_id].evictable_, "Remove failed: Removal called on a non-evictable frame");

  if (data_[frame_id].use_count_ < k_) {
    history_list_.erase(data_[frame_id].pos_);
  } else {
    cache_list_.erase(data_[frame_id].pos_);
  }

  data_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
