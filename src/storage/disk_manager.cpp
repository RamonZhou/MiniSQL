#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->GetMetaData());
  if( meta_page->num_allocated_pages_ < MAX_VALID_PAGE_ID ){
    if( (meta_page->num_extents_ == 0) || 
        (meta_page->extent_used_page_[meta_page->num_extents_ - 1] >= BITMAP_SIZE) ){ meta_page->num_extents_++;}
    char bitmap_data[PAGE_SIZE];
    memset(bitmap_data, 0, PAGE_SIZE);
    uint32_t allo_page_id_in_bitmap = 0;
    ReadPhysicalPage( (meta_page->num_extents_-1) * (BITMAP_SIZE + 1) + 1, bitmap_data);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
    bitmap->AllocatePage(allo_page_id_in_bitmap);
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[meta_page->num_extents_ - 1]++;
    WritePhysicalPage((meta_page->num_extents_-1) * (BITMAP_SIZE + 1) + 1, bitmap_data);
    return (meta_page->num_extents_-1) * BITMAP_SIZE + allo_page_id_in_bitmap;
  }else return INVALID_PAGE_ID;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if(logical_page_id < 0) return;
  if(!IsPageFree(logical_page_id)){
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(this->GetMetaData());
    char bitmap_data[PAGE_SIZE];
    memset(bitmap_data, 0, PAGE_SIZE);
    ReadPhysicalPage((logical_page_id / BITMAP_SIZE ) * (BITMAP_SIZE + 1) + 1, bitmap_data);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
    bitmap->DeAllocatePage(logical_page_id % BITMAP_SIZE);
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
    WritePhysicalPage((logical_page_id / BITMAP_SIZE ) * (BITMAP_SIZE + 1) + 1, bitmap_data);
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if(logical_page_id < 0) return false;
  char bitmap_data[PAGE_SIZE];
  memset(bitmap_data, 0, PAGE_SIZE);
  ReadPhysicalPage((logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1, bitmap_data);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
  return bitmap->IsPageFree(logical_page_id % BITMAP_SIZE);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id/BITMAP_SIZE + 2 + logical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}