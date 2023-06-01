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
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
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
  size_t bucket_idx = IndexOf(key);
  return dir_[bucket_idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t bucket_idx = IndexOf(key);
  return dir_[bucket_idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Grow() {
  int capacity = dir_.size();
  dir_.resize(capacity << 1);
  for (int i = 0; i < capacity; i++) {  // 调整索引
    dir_[i + capacity] = dir_[i];
  }
  global_depth_++;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (dir_[IndexOf(key)]->IsFull()) {  // 需要递归判断是否能将要插入的桶是否为满
    size_t index = IndexOf(key);
    auto target_bucket = dir_[index];
    int bucket_localdepth = target_bucket->GetDepth();
    if (global_depth_ == bucket_localdepth) {
      Grow();
    }
    int mask = 1 << bucket_localdepth;
    // 声明两个桶，废除原来目录中指向的那个桶。
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, bucket_localdepth + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, bucket_localdepth + 1);
    // 重新调整桶中数据
    for (auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }
    num_buckets_++;
    // 调整桶中数据
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) == 0U) {
          dir_[i] = bucket_0;
        } else {
          dir_[i] = bucket_1;
        }
      }
    }
  }
  // 将数据插入桶中
  auto index = IndexOf(key);
  auto target_bucket = dir_[index];
  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &item : list_) {
    if (key == item.first) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  if (list_.empty()) {
    return false;  // 空桶
  }
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    // for (auto &item : list_) {
    if (item.first == key) {
      this->list_.remove(item);
      // latch_.unlock();
      return true;
    }
    return false;
  });
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // latch_.lock();
  if (IsFull()) {
    // latch_.unlock();
    return false;
  }

  // 正常插入
  for (auto &item : list_) {
    if (item.first == key) {
      item.second = value;
      // latch_.unlock();
      return true;
    }
  }

  list_.push_back(std::make_pair(key, value));
  // latch_.unlock();
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
