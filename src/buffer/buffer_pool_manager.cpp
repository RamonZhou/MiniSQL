#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if(page_id == INVALID_PAGE_ID) return nullptr;
  unordered_map<page_id_t, frame_id_t>::iterator iter = page_table_.find(page_id);
  if( iter != page_table_.end() ) {
    replacer_->Pin(iter->second);
    pages_[iter->second].pin_count_++;
    return &pages_[iter->second];
  }
  frame_id_t allocated_frame_id;
  Page *allocated_Page;
  if( !free_list_.empty() ) {
    allocated_frame_id = free_list_.front();
    free_list_.pop_front();
    allocated_Page = &pages_[allocated_frame_id];
  }else{
    if( replacer_->Victim(&allocated_frame_id) ) {
      allocated_Page = &pages_[allocated_frame_id];
      // if( allocated_Page->IsDirty() ) disk_manager_->WritePage(pages_[allocated_frame_id].page_id_ , pages_[allocated_frame_id].GetData());
      // page_table_.erase(pages_[allocated_frame_id].page_id_);
    }
    else allocated_Page = nullptr;
  }
  if( allocated_Page ){
    char page_data[PAGE_SIZE];
    memset(page_data,0,PAGE_SIZE);
    replacer_->Pin(allocated_frame_id);
    // if(allocated_Page->page_id_==4 &&allocated_Page->IsDirty() ) cout<<"Write "<<allocated_Page->page_id_<<endl;
    if(allocated_Page->is_dirty_) disk_manager_->WritePage(allocated_Page->page_id_,allocated_Page->GetData());
    page_table_.erase(allocated_Page->page_id_);
    allocated_Page->ResetMemory();
    allocated_Page->page_id_ = page_id;
    allocated_Page->pin_count_ = 1;
    allocated_Page->is_dirty_ = false;
    disk_manager_->ReadPage(allocated_Page->page_id_,page_data);
    std::memcpy(allocated_Page->GetData(), page_data, PAGE_SIZE);
    page_table_.insert(std::make_pair(page_id, allocated_frame_id));
  }
  return allocated_Page;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t allocated_frame_id;
  Page *allocated_Page;
  if( !free_list_.empty() ) {
    allocated_frame_id = free_list_.front();
    free_list_.pop_front();
    allocated_Page = &pages_[allocated_frame_id];
  }
  else{
    if( replacer_->Victim(&allocated_frame_id) ) { 
      allocated_Page = &pages_[allocated_frame_id];
      if( allocated_Page->IsDirty() ) disk_manager_->WritePage(pages_[allocated_frame_id].page_id_ , pages_[allocated_frame_id].GetData());
      page_table_.erase(pages_[allocated_frame_id].page_id_);
    }
    else allocated_Page = nullptr;
  }
  if( allocated_Page ){
    page_id = AllocatePage();
    allocated_Page->ResetMemory();
    allocated_Page->page_id_ = page_id;
    allocated_Page->pin_count_ = 0;
    allocated_Page->is_dirty_ = false;
    page_table_.insert(std::make_pair(page_id, allocated_frame_id));
  }
  return allocated_Page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  unordered_map<page_id_t, frame_id_t>::iterator iter = page_table_.find(page_id);
  if( iter == page_table_.end()) return true;
  else{
    if( pages_[iter->second].pin_count_ != 0) return false;
    else{
      DeallocatePage(page_id);
      pages_[iter->second].ResetMemory();
      pages_[iter->second].is_dirty_ = false;
      pages_[iter->second].page_id_ = INVALID_PAGE_ID;
      free_list_.emplace_back(iter->first);
      page_table_.erase(page_id);
      return true;
    }
  }

  return false;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  unordered_map<page_id_t, frame_id_t>::iterator iter = page_table_.find(page_id);
  if( iter != page_table_.end() ){
    pages_[iter->second].is_dirty_ |= is_dirty;
    if(pages_[iter->second].pin_count_ > 0) {
      pages_[iter->second].pin_count_--;
      if(pages_[iter->second].pin_count_ == 0) {
        replacer_->Unpin(iter->second);
      }
    }
    return true;
  }
  else return false;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  unordered_map<page_id_t, frame_id_t>::iterator iter = page_table_.find(page_id);
  if( iter != page_table_.end() ){
    disk_manager_->WritePage(page_id,pages_[iter->second].GetData());
    return true;
  }
  else return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}