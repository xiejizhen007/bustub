//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages): num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock_guard(this->mutex_lock_);
    if (this->pin_lists_.size() > 0) {
        *frame_id = this->pin_lists_.back();
        this->pin_map_table_.erase(this->pin_lists_.back());
        this->pin_lists_.pop_back();
        return true;
    }
    return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock_guard(this->mutex_lock_);
    auto iter = this->pin_map_table_.find(frame_id);
    if (iter != this->pin_map_table_.end() && iter->second != this->pin_lists_.end()) {
        // frame_id 存在于哈希表中，并且不等于链表的 end()
        this->pin_count_[frame_id]++;
        this->pin_lists_.erase(iter->second);
        // 将当前的 frame_id 设为 end()，表示 frame_id 是从 lru 移出去的
        this->pin_map_table_[frame_id] = this->pin_lists_.end();
    }
    // nothing
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock_guard(this->mutex_lock_);
    // 1. frame_id 不曾进入过 lru
    // 2. frame_id 被 pin 过了
    auto iter = this->pin_map_table_.find(frame_id);
    if (iter == this->pin_map_table_.end()) {
        // 未曾进入，并且 lru 还有空间
        if (this->pin_lists_.size() < num_pages_) {
            this->pin_count_[frame_id] = 0;
            this->pin_lists_.emplace_front(frame_id);
            this->pin_map_table_[frame_id] = this->pin_lists_.begin();
        }
    } else {
        // frame_id 是被 pin 的
        this->pin_count_[frame_id]--;
        // 只有在身上的 pin count 等于 0 时才有资格进入 lru
        if (this->pin_count_[frame_id] == 0) {
            this->pin_count_[frame_id] = 0;
            this->pin_lists_.emplace_front(frame_id);
            this->pin_map_table_[frame_id] = this->pin_lists_.begin();
        }
    }
}

auto LRUReplacer::Size() -> size_t {
    std::lock_guard<std::mutex> lock_guard(this->mutex_lock_);
    return this->pin_lists_.size();
}
}  // namespace bustub
