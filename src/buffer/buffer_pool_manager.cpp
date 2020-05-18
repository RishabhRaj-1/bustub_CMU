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

    /* load to page table */
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

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  frame_id_t frame;

  /* IF: page NOT found */
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_INFO("Unpin page %d from non-ex", page_id);
    return true;
  }

  /* IF: return false if the page pin count is <= 0 before this call */
  frame = page_table_[page_id];
  if (pages_[frame].GetPinCount() <= 0) {
    LOG_ERROR("Unpin page %d failed, pincnt <= 0", page_id);
    return false;
  }

  /* CASE: the page CAN be unpinned */
  pages_[frame].pin_count_--;
  pages_[frame].is_dirty_ |= is_dirty;

  /* move the page to replacer */
  /* NOTE: can this if be removed */
  if (pages_[frame].GetPinCount() == 0) { /* move the page into replacer only when reflag = 0 */
    replacer_->Unpin(frame);
  }
  LOG_INFO("Unpin page %d from bf, present pin_cnt: %d", page_id, pages_[frame].pin_count_);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  frame_id_t frame;
  // Make sure you call DiskManager::WritePage!

  /* IF: page NOT found */
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_ERROR("Flush page %d failed, not found in page table", page_id);
    return false; /* return false if the page could not be found in the page table */
  }

  /* IF: the page hasn't been modified */
  frame = page_table_[page_id];
  if (!pages_[frame].IsDirty()) {
    LOG_INFO("Flush page %d without dirty", page_id);
    return true;
  }

  /* CASE: the page has been modified, write back to disk first */
  disk_manager_->WritePage(page_id, pages_[frame].data_);
  pages_[frame].is_dirty_ = false;
  LOG_INFO("Flush page %d dirty, write back to disk", page_id);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  /* S2 IF: there's free page in fl, pick a victim page P from fl */
  if (!free_list_.empty()) {
    frame_id_t free_id = free_list_.front();
    free_list_.pop_front();

    /* load to page table */
    *page_id = disk_manager_->AllocatePage();
    pages_[free_id].ResetMemory();
    pages_[free_id].page_id_ = *page_id;
    pages_[free_id].pin_count_ = 1;
    pages_[free_id].is_dirty_ = false;
    replacer_->Pin(free_id);
    page_table_[*page_id] = free_id;

    LOG_INFO("New page %d created from fl", *page_id);
    return &pages_[free_id];
  }

  /* There's NO free page in fl */
  /* S2 CASE: there's free page in replacer, pick a victim page P from replacer */
  frame_id_t candi_id;
  bool evict_suc = replacer_->Victim(&candi_id);
  page_id_t victim_id = pages_[candi_id].GetPageId();

  /* S1 IF: all the pages in the buffer pool are pinned, return nullptr */
  if (!evict_suc) { /* there's NO space in replacer */
    return nullptr;
  }

  /* IF: candi page is dirty, then flush the dirty page */
  if (pages_[candi_id].IsDirty()) {
    bool flu_suc = FlushPageImpl(victim_id);
    /* IF the dirty candi page could not be found in the page table */
    if (!flu_suc) {
      return nullptr;
    }
  }

  /* S3: Update P's metadata, zero out memory and add P to the page table */
  page_table_.erase(victim_id);
  *page_id = disk_manager_->AllocatePage();
  pages_[candi_id].ResetMemory(); /* zero out memory */
  /* add P to the page table */
  pages_[candi_id].page_id_ = *page_id;
  pages_[candi_id].pin_count_ = 1;
  pages_[candi_id].is_dirty_ = false;
  replacer_->Pin(candi_id);
  page_table_[*page_id] = candi_id;

  /* S4: set the page ID output parameter. Return a pointer to P */
  LOG_INFO("New page %d created from replacer", *page_id);
  return &pages_[candi_id];
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
  for (auto i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      FlushPageImpl(pages_[i].GetPageId());
    }
  }
  LOG_INFO("All pages have been flushed!");
}

}  // namespace bustub
