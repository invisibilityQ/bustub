//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <fstream>
#include <iostream>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  // *@brief 找到具有最大后向 k 距离的帧并驱逐该帧。 只有标记为“可驱逐”的帧才是驱逐的候选者。
  // *
  // * 具有少于 k 个历史参考的帧被赋予 +inf 作为其向后 k 距离。 如果多个帧具有 inf 后向 k
  // 距离，则整体驱逐具有最早时间戳的帧。
  // *
  // * 框架的成功驱逐应该减少替换器的大小并删除框架的访问历史记录。
  // *
  // * @param[out] 被逐出的帧的 frame_id id。
  // * @return 如果帧被成功驱逐则为真，如果没有帧可以被驱逐则为假。
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  // @brief 记录在当前时间戳访问给定帧ID的事件。
  //   * 如果以前没有看到帧 ID，则为访问历史创建一个新条目。
  //   *
  //   * 如果帧 ID 无效（即大于 replacer_size_），则抛出异常。 你可以
  //   * 如果帧 ID 无效，也使用 BUSTUB_ASSERT 中止进程。
  //   *
  //   * @param frame_id 接收到新访问的帧的 id。
  //   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  // @brief 切换框架是可驱逐的还是不可驱逐的。 这个功能还有
  //   * 控制替换器的大小。 请注意，大小等于可驱逐条目的数量。
  //   *
  //   * 如果一个框架以前是可回收的并且要设置为不可回收的，那么大小应该递减。
  //   如果一个框架以前是不可回收的并且将被设置为可回收的，那么大小应该增加。
  //   *
  //   * 如果 frame id 无效，则抛出异常或中止进程。
  //   *
  //   * 对于其他场景，这个函数应该在不修改任何东西的情况下终止。
  //   *
  //   * @param frame_id 帧的id，其“可退出”状态将被修改
  //   * @param set_evictable 给定的帧是否可驱逐
  //   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  std::unordered_map<frame_id_t, size_t> access_count_;
  std::list<frame_id_t> history_list_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> history_map_;

  std::list<frame_id_t> cache_list_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> cache_map_;

  std::unordered_map<frame_id_t, bool> is_evictable_;
  size_t k_;
  std::mutex latch_;
};
}  // namespace bustub
