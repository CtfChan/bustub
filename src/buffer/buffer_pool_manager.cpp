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
#include "common/logger.h"

#include <list>
#include <unordered_map>


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
  std::lock_guard<std::mutex> lock(latch_);

  // search for page
  auto page_iter = page_table_.find(page_id);

  // page exists, pin frame and update pin count
  if (page_iter != page_table_.end()) {
    auto frame_id = page_iter->second;
    replacer_->Pin(frame_id); 
    pages_[frame_id].pin_count_++; 
    return pages_ + page_iter->second;
  }

  // page DNE
  auto frame_id = pickVictimPage(); // this function will write to disk as necessary
  if (frame_id < 0) 
    return nullptr;

  // we now have a fresh page, update metadata, read page from disk, insert to page table 
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_.insert({page_id, frame_id});

  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  // find page and unpin it
  std::lock_guard<std::mutex> lock(latch_);

  auto page_iter = page_table_.find(page_id);

  if (page_iter == page_table_.end())
    return false;

  frame_id_t frame_id = page_iter->second;
  Page &page = pages_[frame_id];

  if (page.pin_count_ <= 0)
    return false;

  --page.pin_count_;
  page.is_dirty_ = is_dirty || page.is_dirty_;
  if (page.pin_count_ == 0) 
    replacer_->Unpin(frame_id);

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // This function will just flush target page to disk
  auto page_iter = page_table_.find(page_id);

  if (page_iter != page_table_.end()) {
    frame_id_t frame_id = page_iter->second;
    Page &page = pages_[frame_id];
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
      page.is_dirty_ = false;
    }

    return true;
  }

  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  
  // check all pinned
  if (allPinned())
    return nullptr;
  
  // pick victim
  frame_id_t victim_page = pickVictimPage();
  if (victim_page  < 0) 
    return nullptr;

  // update victim metadata, zero out memory and add to page table
  auto page = pages_ + victim_page;

  page->ResetMemory();

  page->page_id_ = disk_manager_->AllocatePage();
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  page_table_.insert({page->GetPageId(), victim_page});

  // set page ID output param
  *page_id = page->GetPageId();

  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  auto page_iter = page_table_.find(page_id);

  // page DNE
  if (page_iter == page_table_.end()) 
    return false;

  frame_id_t frame_id = page_iter->second;

  // page exists, but non-zero pin-count
  if (pages_[frame_id].pin_count_ != 0)
    return false;

  // page can be deleted
  page_table_.erase(page_id);

  replacer_->Pin(frame_id); // remove from buffer pool since it will no longer be in page table

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  disk_manager_->DeallocatePage(page_id);

  free_list_.emplace_back(static_cast<int>(page_iter->second));

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPageImpl(pages_[i].page_id_);
  }

}

bool BufferPoolManager::allPinned() {
  for (size_t i = 0; i < pool_size_; ++i) {
    auto page = pages_ + i;
    if (page->pin_count_ <= 0) {
      return false;
      break;
    }
  }
  return true;
}


frame_id_t BufferPoolManager::pickVictimPage() {
  frame_id_t free_page;
  // check free list first
  if (free_list_.empty() == false) {
    free_page = static_cast<frame_id_t>(free_list_.front());
    free_list_.pop_front();
    return free_page;
  }

  // then go into replace
  if (replacer_->Victim(&free_page) == false) 
    return -1;

  // if page is in use, flush if necessary, then erase
  auto page = pages_ + free_page; // Page*
  if (page->IsDirty()) 
    FlushPageImpl(page->GetPageId());
  page_table_.erase(page->GetPageId());

  return free_page;
}




}  // namespace bustub
