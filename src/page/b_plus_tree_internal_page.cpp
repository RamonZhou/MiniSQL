#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // if (index < 0 || index >= GetSize()) {
  //   LOG(ERROR) << "Invalid index " << index << "  with size " << GetSize();
  // }
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // if (index < 0 || index >= GetSize()) {
  //   LOG(ERROR) << "Invalid index " << index;
  //   return;
  // }
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = GetSize();
  for (int i = 0; i < size; ++ i) {
    if (array_[i].second == value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  // if (index < 0 || index >= GetSize()) {
  //   LOG(ERROR) << "Invalid index " << index;
  // }
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  int size = GetSize();
  for (int i = 1; i < size; ++ i) {
    if (comparator(key, array_[i].first) < 0) {
      return array_[i - 1].second;
    }
  }
  return array_[size - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetSize(2);
  array_[0].second = old_value;
  array_[1] = std::make_pair(new_key, new_value);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(INVALID_PAGE_ID);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int size = GetSize();
  for (int i = size - 1; i >= 0; -- i) {
    if (!(array_[i].second == old_value)) {
      array_[i + 1] = array_[i];
    } else {
      array_[i + 1] = std::make_pair(new_key, new_value);
      break;
    }
  }
  SetSize(++ size);
  return size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // auto *recipient_page = buffer_pool_manager->FetchPage(recipient->GetPageId());
  // if (recipient_page == nullptr) {
  //   ASSERT("Recipient page invalid!");
  //   return;
  // }
  // BPlusTreeInternalPage *recipient_node = reinterpret_cast<BPlusTreeInternalPage *>(recipient_page->GetData());
  int size = GetSize();
  recipient->CopyNFrom(array_ + (size - size / 2), size / 2, buffer_pool_manager);
  SetSize(size - size / 2);
  // buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  SetSize(size);
  for (int i = 0; i < size; ++ i) {
    array_[i] = items[i];
    auto *child_page = buffer_pool_manager->FetchPage((page_id_t)items[i].second);
    if (child_page == nullptr) continue;
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage((page_id_t)items[i].second, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  if (index < 0) {
    puts("GGGGGGGGGGGGGGGGGGGGGGGGG");
    // return;
  }
  for (int i = index; i < size - 1; ++ i) {
    array_[i] = array_[i + 1];
  }
  SetSize(-- size);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  int size = GetSize();
  SetSize(-- size);
  return array_[0].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // assert(GetParentPageId() == recipient->GetParentPageId());
  // page_id_t parent_id = GetParentPageId();
  // auto *parent_page = buffer_pool_manager->FetchPage(parent_id);
  // BPlusTreeInternalPage *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  // int mindex = parent_node->ValueIndex(GetPageId());
  // int rindex = parent_node->ValueIndex(recipient->GetPageId());
  // buffer_pool_manager->UnpinPage(parent_id, false);
  // if (mindex == rindex - 1) {
    // int size = GetSize();
    // for (int i = size - 1; i >= 0; -- i) {
    //   MoveLastToFrontOf(recipient, middle_key, buffer_pool_manager);
    // }
  // } else if (mindex == rindex + 1) {
    int size = GetSize();
    KeyType cur_midkey = middle_key;
    for (int i = 0; i < size; ++ i) {
      MoveFirstToEndOf(recipient, cur_midkey, buffer_pool_manager);
      if (i < size - 1) cur_midkey = KeyAt(0);
    }
  // } else {
  //   printf("Not a sibling!\n")
  // }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  auto *recipient_page = buffer_pool_manager->FetchPage(recipient->GetPageId());
  if (recipient_page == nullptr) return;
  BPlusTreeInternalPage *recipient_node = reinterpret_cast<BPlusTreeInternalPage *>(recipient_page->GetData());
  recipient_node->CopyLastFrom(std::make_pair(middle_key, array_[0].second), buffer_pool_manager);
  int size = GetSize();
  for (int i = 0; i < size - 1; ++ i) {
    array_[i] = array_[i + 1];
  }
  SetSize(-- size);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array_[size ++] = pair;
  SetSize(size);
  auto *child_page = buffer_pool_manager->FetchPage((page_id_t)pair.second);
  if (child_page == nullptr) return;
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage((page_id_t)pair.second, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  auto *recipient_page = buffer_pool_manager->FetchPage(recipient->GetPageId());
  if (recipient_page == nullptr) return;
  BPlusTreeInternalPage *recipient_node = reinterpret_cast<BPlusTreeInternalPage *>(recipient_page->GetData());
  int size = GetSize();
  recipient_node->CopyFirstFrom(array_[size - 1], buffer_pool_manager);
  recipient_node->SetKeyAt(1, middle_key);
  SetSize(-- size);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  for (int i = size - 1; i >= 0; -- i) {
    array_[i + 1] = array_[i];
  }
  SetSize(++ size);
  array_[0] = pair;
  auto *child_page = buffer_pool_manager->FetchPage((page_id_t)pair.second);
  if (child_page == nullptr) return;
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage((page_id_t)pair.second, true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;