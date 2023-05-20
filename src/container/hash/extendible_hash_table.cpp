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
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 先保存着 ，待会随着深度变化，再次调用结果可能会不一致
  auto index = IndexOf(key);
  auto bucket = dir_[index];
  // 有剩余空间则直接插入
  if (bucket->Insert(key, value)) {
    // 这里是唯一正确的出口
    return;
  }
  // 开始重整
  // 若局部深度等于全局深度，则更新全局深度与目录容量
  if (bucket->GetDepth() == GetGlobalDepthInternal()) {
    size_t len = dir_.size();
    dir_.reserve(2 * len);
    std::copy_n(dir_.begin(), len, std::back_inserter(dir_));
    global_depth_++;
  }
  // 莫名其妙到这里 item 失效了 原来是 vec 的自动扩容，导致本来的地址不能用了要重新获取 bucket, index
  // 也会变，要用之前的
  bucket = dir_[index];
  // 若局部深度小于全局深度，则更新局部深度并拿个新 Bucket 用
  if (bucket->GetDepth() < GetGlobalDepthInternal()) {
    // 开始分裂
    auto bucket0 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    num_buckets_++;
    // 点睛之笔 比如 当前深度为 1，你不可能 把 1 左移 1位去判别兄弟节点，你应该把有效位的最后一位拿来
    int mask = 1 << bucket->GetDepth();
    for (auto &[k, v] : bucket->GetItems()) {
      size_t hashkey = std::hash<K>()(k);
      // 根据本地深度表示的有效位最后一位判别，若为 1，插入新建的那个bucket
      if ((hashkey & mask) != 0) {
        bucket1->Insert(k, v);
      } else {
        // 根据本地深度表示的有效位最后一位判别，若为 0，插入原有的 bucket
        bucket0->Insert(k, v);
      }
    }
    // 减少不必要的遍历，从前一位的大小开始，若是 0 则从 0 开始 ，若是 1 ，则可以节省很多，兄弟节点这一位肯定是一样的
    for (size_t i = index & (mask - 1); i < dir_.size(); i += mask) {
      // 根据本地深度表示的有效位最后一位判别，若为 1，取代新建的那个bucket
      if ((i & mask) != 0) {
        dir_[i] = bucket1;
      } else {
        // 根据本地深度表示的有效位最后一位判别，若为 0，取代原有的 bucket
        dir_[i] = bucket0;
      }
    }
    latch_.unlock();
    // 递归插入本来要插入的值
    Insert(key, value);
    latch_.lock();
  }
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
