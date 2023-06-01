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

// 判断所给页面的访问次数是否大于k，如果大于即unevcitable不可被置换出去，
// 否则为可淘汰evit出去
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); ++it) {
    if (is_evictable_[*it]) {
      *frame_id = *it;
      access_count_[*frame_id] = 0;
      history_list_.erase(history_map_[*frame_id]);
      history_map_.erase(*frame_id);
      is_evictable_[*frame_id] = false;
      curr_size_--;
      return true;
    }
  }
  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); ++it) {
    if (is_evictable_[*it]) {
      *frame_id = *it;
      access_count_[*frame_id] = 0;
      cache_list_.erase(cache_map_[*frame_id]);
      cache_map_.erase(*frame_id);
      is_evictable_[*frame_id] = false;
      curr_size_--;
      return true;
    }
  }
  return false;
}

// 更新所给的页面的状态，如果其已经在缓存中，则根据其最近访问次数判断是否置为unevitable。
// 如果其没有在缓存中，将调用remove函数将合适的页面置换出去。
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  access_count_[frame_id]++;
  if (access_count_[frame_id] == k_) {  // 放入cache中去
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (access_count_[frame_id] > k_) {  // 已经在cache中，则将其放入cache_list_的对首，表示访问
    if (access_count_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {  // 首次访问
    if (access_count_[frame_id] == 1) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

// 如果当前页面访问次数大于k次，应该将其设置为unevitable，
// 我们可以用一个新的队列来保存访问次数大于k的页面
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (access_count_[frame_id] == 0) {
    return;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (access_count_[frame_id] == 0) {
    return;
  }
  if (access_count_[frame_id] >= k_) {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  } else {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  }
  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}
}  // namespace bustub
