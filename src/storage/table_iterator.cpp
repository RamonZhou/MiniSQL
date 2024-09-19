#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(RowId id,TableHeap *table_heap_) :row(INVALID_ROWID)
{
  this->id = id;
  this->table_heap_ = table_heap_;
}

TableIterator::TableIterator(const TableIterator &other) :row(INVALID_ROWID)
{
  this->id = other.id;
  this->table_heap_ = other.table_heap_;
}

TableIterator::~TableIterator() {
  
}

const Row &TableIterator::operator*() {
  this->row.SetRowId(id);
  table_heap_->GetTuple(&this->row,nullptr);
  this->row.SetRowId(id);
  return this->row;
}

Row *TableIterator::operator->() {
  this->row.SetRowId(id);
  table_heap_->GetTuple(&this->row,nullptr);
  this->row.SetRowId(id);
  return &this->row;
}

TableIterator &TableIterator::operator++() {
  RowId next_row_id;
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(id.GetPageId()));
  //1.判断给出的rid是否能找到page
  if(!page) {
    LOG(INFO) << "CAN'T FIND PAGE IN TableIterator::operator++ FUNCTION";
    id = INVALID_ROWID;
    return *this;
  }
  //2.根据page找到下一条tuple
  //2.1 是否能在当前页找到
  if(page->GetNextTupleRid(id,&next_row_id)){//在这页找到了下一条tuple
    id = next_row_id;
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  }
  //2.2 当前页找不到，需要去下一页找  
  else {
    page_id_t next_page_id = page->GetNextPageId();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);//需要去下一页，先释放本页
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));//找下一页
    while(page != nullptr){ //如果下一页存在
      if(page->GetFirstTupleRid(&next_row_id)){ //如果下一页存在一条记录
        id = next_row_id;
        break;
      }else{
        page_id_t next_page_id = page->GetNextPageId();
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));//找下一页
      }
    }
    // std::cerr<<"+"<<std::endl;
    if (page != nullptr) table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);//把下一页释放
    else id = INVALID_ROWID;
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator result(*this); //先记录一下当前
  ++(*this);//更改当前数据
  return result;//返回更改前的数据
}
