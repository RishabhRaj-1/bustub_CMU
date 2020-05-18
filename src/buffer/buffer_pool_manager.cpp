//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

#include "common/logger.h" /* for debugging, delete after pass all the test */

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  /* S1: Search the page table for the requested page (P) */
  /* S1.1: IF P exists, pin it and return it immediately */
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t p_requested = page_table_[page_id]; /* the requested page (P) */

    replacer_->Pin(p_requested); /* pin it */
    pages_[p_requested].pin_count_ += 1;

    LOG_INFO("Fetch page %d from mem", page_id);
    return &pages_[p_requested];
  }
  /* S1.2: If P does NOT exist, find a replacement page (R) */
  frame_id_t r_target; /* replacement page (R) */

  /* S1.2 IF: search the free list first */
  if (!free_list_.empty()) {
    r_target = free_list_.front();
    free_list_.pop_front();
    replacer_->Pin(r_target);
    pages_[r_target].pin_count_++; /* all in-memory pages in the system are represented by Page */

    /* read to buffer */
    pages_[r_target].page_id_ = page_id;
    pages_[r_target].is_dirty_ = false;
    page_table_[page_id] = r_target;
    disk_manager_->ReadPage(page_id, pages_[r_target].data_);

    LOG_INFO("Fetch page %d from the fl", page_id);
    return &pages_[r_target];
  }

  /* S1.2 ELSE: search the replacer if not found in fl */
  bool evi_suc = replacer_->Victim(&r_target);         /* find the victim */
  page_id_t evict_page = pages_[r_target].GetPageId(); /* get the victim page id */

  /* IF no victim was found */
  if (!evi_suc) {
    return nullptr;
  }

  /* S2 IF: R is dirty, write it back to the disk */
  if (pages_[r_target].IsDirty()) {           /* page in memory has been modified from that on disk */
    bool flu_suc = FlushPageImpl(evict_page); /* flush the victim page to disk first */
    /* IF evict_page could not be found in the page table */
    if (!flu_suc) {
      return nullptr;
    }
  }

  replacer_->Pin(r_target);
  pages_[r_target].pin_count_ += 1;

  /* S3: delete R from the page table and insert P */
  page_table_.erase(evict_page);
  pages_[r_target].page_id_ = page_id; /* read to buffer */
  pages_[r_target].is_dirty_ = false;
  page_table_[page_id] = r_target;
  disk_manager_->ReadPage(page_id, pages_[r_target].data_);

  return &pages_[r_target];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { return false; }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
