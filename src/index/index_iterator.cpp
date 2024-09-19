#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, unsigned int index, const KeyComparator &comparator,
                                                           BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id),
      comparator_(comparator),
      buffer_pool_manager_(buffer_pool_manager) {
  auto *page = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  value_ = node->GetItem(index);
  buffer_pool_manager_->UnpinPage(page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return value_;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  auto *page = buffer_pool_manager_->FetchPage(page_id_);
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  int index = node->KeyIndex(value_.first, comparator_);
  if (comparator_(value_.first, node->KeyAt(index)) == 0) ++ index;
  if (index < node->GetSize()) {
    value_ = node->GetItem(index);
    buffer_pool_manager_->UnpinPage(page_id_, false);
  } else {
    page_id_t next_id = node->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_id_, false);
    page_id_ = next_id;
    if (next_id == INVALID_PAGE_ID) return *this;
    auto *page = buffer_pool_manager_->FetchPage(next_id);
    if (page == nullptr) return *this;
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    value_ = node->GetItem(0);
    buffer_pool_manager_->UnpinPage(next_id, false);
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return (page_id_ == itr.page_id_) && comparator_(value_.first, itr.value_.first) == 0 && (value_.second == itr.value_.second);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
