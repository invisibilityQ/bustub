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

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //    "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

// @brief 在缓冲池中创建一个新页面。 将 page_id 设置为新页面的
// id，如果所有框架当前都在使用中且不可清除（换句话说，固定），则设置为 nullptr。
//     *
//     * 您应该从空闲列表或替换器中选择替换框架（总是首先从空闲列表中找到），
// 然后调用 AllocatePage() 方法来获取新的页面 ID。 如果替换帧有脏页，你应该先把它写回磁盘。
// 您还需要为新页面重置内存和元数据。
//     *
//     * 请记住通过调用 replacer.SetEvictable(frame_id, false)
//     来“固定”框架，这样替换器就不会在缓冲池管理器“取消固定”它之前驱逐该框架。
//     另外，请记住在替换器中记录帧的访问历史，以便 lru-k 算法工作。
//     *
//     * @param[out] page_id 创建页面的id
//     * @return nullptr 如果不能创建新页面，否则指向新页面的指针
//     */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t i = 0;
  for (; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      break;
    }
  }
  if (i == pool_size_) {
    return nullptr;
  }

  frame_id_t frame_id;
  *page_id = AllocatePage();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // }else if (!replacer_->Evict(&frame_id)){
    //   std::cout<<"replacer->evict false"<<std::endl;
    //   return nullptr;
  } else {
    replacer_->Evict(&frame_id);
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evict_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_->Remove(evict_page_id);
  }

  page_table_->Insert(*page_id, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;

  return &pages_[frame_id];
}

// @brief 从缓冲池中获取请求的页面。 如果 page_id
// 需要从磁盘中获取，但所有帧当前都在使用中且不可清除（换句话说，固定），则返回 nullptr。
//     *
//     * 首先在缓冲池中查找page_id。 如果没有找到，从空闲列表或替换器中选择一个替换帧（总是首先从空闲列表中找到），
// 通过调用 disk_manager_->ReadPage() 从磁盘读取页面，并替换帧中的旧页面。
//  与NewPgImp()类似，如果旧页面脏了，需要将其写回磁盘并更新新页面的元数据
//     *
//     * 此外，请记住禁用驱逐并记录框架的访问历史记录，就像您对 NewPgImp() 所做的那样。
//     *
//     * @param page_id 要获取的页面的id
//     * @return nullptr 如果无法获取 page_id，否则指向请求页面的指针
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  size_t i = 0;
  for (; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      break;
    }
  }
  if (i == pool_size_) {
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // }
    // if (!replacer_->Evict(&frame_id)){
    //   return nullptr;
  } else {
    replacer_->Evict(&frame_id);
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evict_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();

    page_table_->Remove(evict_page_id);
  }

  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

// /**
//     * TODO(P1): 添加实现
//     *
//     * @brief 从缓冲池中取消固定目标页面。 如果 page_id 不在缓冲池中或者其 pin 计数已经为 0，则返回 false。
//     *
//     * 减少页面的引脚数。 如果 pin 数达到 0，则该帧应该可以被替换器逐出。
//     此外，在页面上设置脏标志以指示页面是否被修改。
//     *
//     * @param page_id 要取消固定的页面的 id
//     * @param is_dirty 如果页面应该被标记为脏，则为 true，否则为 false
//     * @return false 如果页面不在页表中或者在调用之前它的 pin 计数 <= 0，否则为 true
//     */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

/**
 * TODO(P1): 添加实现
 *
 * @brief 将目标页面刷新到磁盘。
 *
 * 使用 DiskManager::WritePage() 方法将页面刷新到磁盘，不管是否有脏标志。
 * 刷新后取消设置页面的脏标志。
 *
 * @param page_id 要刷新的页面的id，不能是INVALID_PAGE_ID
 * @return false 如果在页表中找不到该页，否则为 true
 */
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID) {
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  // pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || page_id == INVALID_PAGE_ID) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  page_table_->Remove(page_id);
  free_list_.emplace_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
