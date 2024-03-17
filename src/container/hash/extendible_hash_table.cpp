//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto idx = IndexOf(key);
  V res_val{};
  if (this->dir_[idx]->Find(key, res_val)) {
    value = res_val;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto idx = IndexOf(key);
  return this->dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    auto bin_idx = IndexOf(key);
    auto cur_bucket = dir_[bin_idx];
    if (cur_bucket->Insert(key, value)) {
      break;
    }
    if (cur_bucket->GetDepth() == GetGlobalDepthInternal()) {  // equal, do dir grow
      global_depth_++;
      auto sz = dir_.size();
      for (size_t i = 0; i < sz; i++) {  // double dir size
        dir_.push_back(dir_[i]);
      }  // split will do next time
    } else {
      RedistributeBucket(cur_bucket);  // increase local_dep, rearrange previous stored data
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto ori_dep = bucket->GetDepth();
  bucket->IncrementDepth();  // first increase local
  // now get a new bucket and rearrange values
  auto new_bkt = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  auto old_bkt_element = bucket->GetItems();
  this->num_buckets_++;
  int mask = (1 << ori_dep) - 1;
  auto rand_key = (*(bucket->GetItems().begin())).first;
  size_t ori_idx = std::hash<K>()(rand_key) & mask;
  for (auto &[k, v] : old_bkt_element) {
    auto cur_idx = std::hash<K>()(k) & ((1 << bucket->GetDepth()) - 1);
    if (cur_idx != ori_idx) {  // position changed, erase & insert
      new_bkt->Insert(k, v);
      bucket->Remove(k);
    }
  }
  // get dir redirect
  // add: originally same && currently different(index) bucket to new place
  for (size_t i = 0; i < dir_.size(); i++) {
    auto ori_bit = i & ((1 << (bucket->GetDepth() - 1)) - 1);
    auto new_bit = i & ((1 << bucket->GetDepth()) - 1);
    if (ori_bit == ori_idx && new_bit != ori_idx) {
      dir_[i] = new_bkt;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&](auto cur) -> bool {
    if (cur.first == key) {
      value = cur.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&](auto cur) {
    if (cur.first == key) {
      list_.remove(cur);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  if (this->IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
