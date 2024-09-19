#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(sizeof(row) > PAGE_SIZE) return false;
  auto iter = first_page_id_;
  page_id_t last_page_id = INVALID_PAGE_ID ,insert_page_id;
  TablePage *last_page;
  TablePage *page = nullptr;
  //find a page to insert
  for(iter = first_page_id_; iter != INVALID_PAGE_ID; iter = page->GetNextPageId()){
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(iter));
    page->WLatch();
    
    if( page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_) ){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      page->WUnlatch();
      
      return true;
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    last_page_id = iter;
  }
  
  //no free page
  //try to allocate a new page
  page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(insert_page_id));
  if(page == nullptr) return false; //allocate failed, no memory
  //modify last page
  last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id));
  if(last_page != nullptr){
    last_page->WLatch();
    last_page->WUnlatch();
    last_page->SetNextPageId(insert_page_id);
    buffer_pool_manager_->UnpinPage(last_page_id, true);
  }
  //modify this page
  page->WLatch();
  page->Init(insert_page_id,last_page_id,log_manager_,txn);
  page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(insert_page_id, true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  if(sizeof(row) > PAGE_SIZE) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row(rid);
  if(page->GetTuple(&old_row,schema_,txn,lock_manager_)){
    old_row.SetRowId(rid);
    page->WLatch();
    if(page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_)){
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
  }
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  buffer_pool_manager_->~BufferPoolManager();
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    row = nullptr;
    return false;
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return page->GetTuple(row,schema_,txn,lock_manager_);
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId first_row_id_;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->GetFirstTupleRid(&first_row_id_);
  buffer_pool_manager_->UnpinPage(first_page_id_,false);
  return TableIterator(first_row_id_,this);
}

TableIterator TableHeap::End() {
  RowId tmp_rowid(INVALID_PAGE_ID,0);
  return TableIterator(tmp_rowid,this);
}
