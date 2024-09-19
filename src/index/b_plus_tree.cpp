#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  auto *roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto *roots_page_data = reinterpret_cast<IndexRootsPage *>(roots_page->GetData());
  if (!roots_page_data->GetRootId(index_id_, &root_page_id_)) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  } else {
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  // while(!IsEmpty()) {
  //   Remove((*Begin()).first);
  // }
  // sleep(1);
  root_page_id_ = INVALID_PAGE_ID;
  auto *roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *roots_node = reinterpret_cast<IndexRootsPage *>(roots_page->GetData());
  roots_node->Delete(index_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  page_id_t cur_page_id = root_page_id_;
  while (true) {
    if (cur_page_id == INVALID_PAGE_ID) return false;
    auto *cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
    if (cur_page == nullptr) return false;
    BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    bool flag = true;
    if (cur_node->IsLeafPage()) {
      LeafPage *cur_node = reinterpret_cast<LeafPage *>(cur_page->GetData());
      ValueType val;
      flag = cur_node->Lookup(key, val, comparator_);
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      if (flag) result.push_back(val);
      return flag;
    } else {
      InternalPage *cur_node = reinterpret_cast<InternalPage *>(cur_page->GetData());
      page_id_t child;
      child = cur_node->Lookup(key, comparator_);
      cur_page_id = child;
      buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), false);
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else {
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    page_id_t new_page_id = INVALID_PAGE_ID;
    auto *new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr) {
      printf("StartNewTree oom\n");
      // throw "Out of memory";
      return;
    }
    LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    root_page_id_ = new_page_id;
    UpdateRootPageId(0);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  page_id_t cur_page_id = root_page_id_;
  Page *cur_page = nullptr;
  while (true) {
    cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
    if (cur_page == nullptr) {
      printf("InsertIntoLeaf phase 1 oom\n");
      // throw "Out of memory";
      return false;
    }
    BPlusTreePage *cur_node_head = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    if (cur_node_head->IsLeafPage()) {
      break;
    } else {
      InternalPage *cur_node = reinterpret_cast<InternalPage *>(cur_page->GetData());
      page_id_t child_page_id = cur_node->Lookup(key, comparator_);
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      cur_page_id = child_page_id;
    }
  }

  LeafPage *leaf = reinterpret_cast<LeafPage *>(cur_page->GetData());
  ValueType val;
  if (leaf->Lookup(key, val, comparator_)) {
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    printf("Duplicated key.\n");
    return false;
  }
  page_id_t merge_left = cur_page_id;
  page_id_t merge_right = INVALID_PAGE_ID;
  KeyType left_key = leaf->KeyAt(0);
  KeyType right_key;
  if (leaf->Insert(key, value, comparator_) > leaf_max_size_) {
    LeafPage *split_leaf = Split(leaf);
    merge_right = split_leaf->GetPageId();
    right_key = split_leaf->KeyAt(0);
    buffer_pool_manager_->UnpinPage(split_leaf->GetPageId(), true);
  }
  cur_page_id = leaf->GetParentPageId();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);

  // modify up to the root
  while (true) {
    // if is top of the root
    if (cur_page_id == INVALID_PAGE_ID) {
      if (merge_left != root_page_id_) {
        printf("InsertIntoLeaf uke\n");
        // throw "UKE";
        return false;
      }
      if (merge_right == INVALID_PAGE_ID) break;
      else {
        // populate new root
        page_id_t new_page_id = INVALID_PAGE_ID;
        auto *new_page = buffer_pool_manager_->NewPage(new_page_id);
        if (new_page == nullptr) {
          printf("InsertIntoLeaf phase 2 oom\n");
          // throw "Out of memory";
          return false;
        }
        InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
        new_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
        new_node->PopulateNewRoot(merge_left, right_key, merge_right);
        root_page_id_ = new_page_id;
        UpdateRootPageId(0);
        // left
        auto *child_page = buffer_pool_manager_->FetchPage(merge_left);
        InternalPage *child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
        child_node->SetParentPageId(root_page_id_);
        buffer_pool_manager_->UnpinPage(merge_left, true);
        // right
        child_page = buffer_pool_manager_->FetchPage(merge_right);
        child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
        child_node->SetParentPageId(root_page_id_);
        buffer_pool_manager_->UnpinPage(merge_right, true);
        // unpin root
        buffer_pool_manager_->UnpinPage(new_page_id, true);
        break;
      }
    }
    cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
    InternalPage *cur_node = reinterpret_cast<InternalPage *>(cur_page->GetData());
    if (cur_node == nullptr) {
      printf("InsertIntoLeaf phase 3 oom\n");
      // throw "Out of memory";
      return false;
    }
    int left_index = cur_node->ValueIndex(merge_left);
    cur_node->SetKeyAt(left_index, left_key);
    if (merge_right != INVALID_PAGE_ID) {
      if (cur_node->InsertNodeAfter(merge_left, right_key, merge_right) > internal_max_size_) {
        InternalPage *split_node = Split(cur_node);
        merge_right = split_node->GetPageId();
        right_key = split_node->KeyAt(0);
        buffer_pool_manager_->UnpinPage(split_node->GetPageId(), true);
      } else {
        merge_right = INVALID_PAGE_ID;
      }
    }
    merge_left = cur_node->GetPageId();
    left_key = cur_node->KeyAt(0);
    cur_page_id = cur_node->GetParentPageId();
    buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), true);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    printf("Split oom\n");
    throw "Out of memory";
    return nullptr;
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  if (node->IsLeafPage()) {
    LeafPage *node_ = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_node_ = reinterpret_cast<LeafPage *>(new_node);
    new_node_->Init(new_page_id, node_->GetParentPageId(), leaf_max_size_);
    new_node_->SetNextPageId(node_->GetNextPageId());
    node_->SetNextPageId(new_page_id);
    node_->MoveHalfTo(new_node_);
  } else {
    InternalPage *node_ = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_node_ = reinterpret_cast<InternalPage *>(new_node);
    new_node_->Init(new_page_id, node_->GetParentPageId(), internal_max_size_);
    node_->MoveHalfTo(new_node_, buffer_pool_manager_);
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  page_id_t cur_page_id = root_page_id_;
  Page *cur_page = nullptr;
  // search for key
  while (true) {
    cur_page = buffer_pool_manager_->FetchPage(cur_page_id);
    if (cur_page == nullptr) {
      printf("InsertIntoLeaf phase 1 oom\n");
      // throw "Out of memory";
      return;
    }
    BPlusTreePage *cur_node_head = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    if (cur_node_head->IsLeafPage()) {
      break;
    } else {
      InternalPage *cur_node = reinterpret_cast<InternalPage *>(cur_page->GetData());
      page_id_t child_page_id = cur_node->Lookup(key, comparator_);
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      cur_page_id = child_page_id;
    }
  }
  // deal with leaf
  LeafPage *leaf = reinterpret_cast<LeafPage *>(cur_page->GetData());
  page_id_t leaf_id = leaf->GetPageId();
  ValueType val;
  if (!leaf->Lookup(key, val, comparator_)) {
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    return;
  }
  leaf->RemoveAndDeleteRecord(key, comparator_);
  if (CoalesceOrRedistribute(leaf, transaction)) {
    buffer_pool_manager_->UnpinPage(leaf_id, true);
    buffer_pool_manager_->DeletePage(leaf_id);
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->GetParentPageId() == INVALID_PAGE_ID) {
    assert(node->GetPageId() == root_page_id_);
    return AdjustRoot(node);
  }

  // page_id_t cur_id = node->GetPageId();
  // node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(cur_id)->GetData());
  // do {
  //   auto *page = buffer_pool_manager_->FetchPage(node->GetPageId());
  //   InternalPage *knode = reinterpret_cast<InternalPage *>(page->GetData());
  //   if (knode->GetParentPageId() != node->GetParentPageId()) cout << "!" << node->GetPageId() << " - " << knode->GetPageId() << " * " << knode->GetParentPageId() << " = " << node->GetParentPageId() << endl;
  //   buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  // }while(0);
  page_id_t parent_id = node->GetParentPageId();
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int mindex = parent_node->ValueIndex(node->GetPageId());
  int msize = node->GetSize();
  int psize = parent_node->GetSize();
  parent_node->SetKeyAt(mindex, node->KeyAt(0));
  if (msize >= node->GetMinSize()) {
    if (mindex == 0) {
      CoalesceOrRedistribute(parent_node, transaction);
    }
    return false;
  }
  // try to redistribute
  //    node -> sibling
  if (mindex < psize - 1) {
    int rindex = mindex + 1;
    page_id_t sibling_id = parent_node->ValueAt(rindex);
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    int ssize = sibling_node->GetSize();
    if ((msize + ssize) / 2 >= node->GetMinSize()) {
      Redistribute(sibling_node, node, 0);
      // parent_node->SetKeyAt(mindex, node->KeyAt(0));
      parent_node->SetKeyAt(rindex, sibling_node->KeyAt(0));
      [[maybe_unused]] bool flag = false;
      if (mindex == 0 || rindex == 0) {
        flag = CoalesceOrRedistribute(parent_node, transaction);
        assert(flag == false);
      }
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // buffer_pool_manager_->UnpinPage(cur_id, true);
      return false;
    } else {
      buffer_pool_manager_->UnpinPage(sibling_id, false);
    }
  }
  //    sibling <- node
  if (mindex > 0) {
    int rindex = mindex - 1;
    page_id_t sibling_id = parent_node->ValueAt(rindex);
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    int ssize = sibling_node->GetSize();
    if ((msize + ssize) / 2 >= node->GetMinSize()) {
      Redistribute(sibling_node, node, 1);
      // parent_node->SetKeyAt(rindex, sibling_node->KeyAt(0));
      parent_node->SetKeyAt(mindex, node->KeyAt(0));
      [[maybe_unused]] bool flag = false;
      if (mindex == 0 || rindex == 0) {
        flag = CoalesceOrRedistribute(parent_node, transaction);
        assert(flag == false);
      }
      buffer_pool_manager_->UnpinPage(sibling_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // buffer_pool_manager_->UnpinPage(cur_id, true);
      return false;
    } else {
      buffer_pool_manager_->UnpinPage(sibling_id, false);
    }
  }
  // try to coalesce
  //    node -> sibling
  if (mindex < psize - 1) {
    int rindex = mindex + 1;
    page_id_t sibling_id = parent_node->ValueAt(rindex);
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    // int ssize = sibling_node->GetSize();
    bool flag = false;
    if (Coalesce(&sibling_node, &node, &parent_node, 0, transaction) || mindex == 0) {
      flag = CoalesceOrRedistribute(parent_node, transaction);
    }
    buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->DeletePage(sibling_node->GetPageId());
      // buffer_pool_manager_->UnpinPage(cur_id, true);
    if (flag) buffer_pool_manager_->DeletePage(parent_id);
    return false;
  }
  //    sibling <- node
  if (mindex > 0) {
    int rindex = mindex - 1;
    page_id_t sibling_id = parent_node->ValueAt(rindex);
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
    // int ssize = sibling_node->GetSize();
    bool flag = false;
    if (Coalesce(&sibling_node, &node, &parent_node, 1, transaction) || rindex == 0) {
      flag = CoalesceOrRedistribute(parent_node, transaction);
    }
    buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
      // buffer_pool_manager_->UnpinPage(cur_id, true);
    if (flag) buffer_pool_manager_->DeletePage(parent_id);
    return true;
  }
      // buffer_pool_manager_->UnpinPage(cur_id, true);
  printf("Err COD %d [%d, %d] (%d, %d)\n", mindex, 0, psize - 1, node->GetPageId(), parent_id);
  assert(0);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // LOG(INFO) << "Coalesce" << std::endl;
  // make it to neighbor_node <- node
  // index == 0: node -> sibling
  // page_id_t sid = reinterpret_cast<BPlusTreePage *>(*neighbor_node)->GetPageId();
  // page_id_t nid = reinterpret_cast<BPlusTreePage *>(*node)->GetPageId();
  if (index == 0) {
    N **tmp = neighbor_node;
    neighbor_node = node;
    node = tmp;
  }
  if ((*node)->IsLeafPage()) {
    LeafPage *inode = reinterpret_cast<LeafPage *>(*node);
    LeafPage *isibling = reinterpret_cast<LeafPage *>(*neighbor_node);
    InternalPage *parent_node = *parent;
    inode->MoveAllTo(isibling);
    isibling->SetNextPageId(inode->GetNextPageId());
    parent_node->Remove(parent_node->ValueIndex(inode->GetPageId()));
    return parent_node->GetSize() < parent_node->GetMinSize();
  } else {
    InternalPage *inode = reinterpret_cast<InternalPage *>(*node);
    InternalPage *isibling = reinterpret_cast<InternalPage *>(*neighbor_node);
    InternalPage *parent_node = *parent;
    inode->MoveAllTo(isibling, parent_node->KeyAt(parent_node->ValueIndex(inode->GetPageId())), buffer_pool_manager_);
    parent_node->Remove(parent_node->ValueIndex(inode->GetPageId()));
    return parent_node->GetSize() < parent_node->GetMinSize();
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // LOG(INFO) << "Redistribute" << std::endl;
  if (!node->IsLeafPage()) {
    InternalPage *inode = reinterpret_cast<InternalPage *>(node);
    InternalPage *isibling = reinterpret_cast<InternalPage *>(neighbor_node);
    page_id_t parent_id = inode->GetParentPageId();
    auto *parent_page = buffer_pool_manager_->FetchPage(parent_id);
    InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    KeyType middle_key = parent_node->KeyAt(parent_node->ValueIndex(
      index == 0 ? isibling->GetPageId() : inode->GetPageId()
    ));
    buffer_pool_manager_->UnpinPage(parent_id, false);
    while (abs(inode->GetSize() - neighbor_node->GetSize()) > 1) {
      if (index == 0) {
        isibling->MoveFirstToEndOf(inode, middle_key, buffer_pool_manager_);
        middle_key = isibling->KeyAt(0);
      } else {
        isibling->MoveLastToFrontOf(inode, middle_key, buffer_pool_manager_);
        middle_key = inode->KeyAt(0);
      }
    }
  } else {
    LeafPage *inode = reinterpret_cast<LeafPage *>(node);
    LeafPage *isibling = reinterpret_cast<LeafPage *>(neighbor_node);
    while (abs(inode->GetSize() - neighbor_node->GetSize()) > 1) {
      if (index == 0) isibling->MoveFirstToEndOf(inode);
      else isibling->MoveLastToFrontOf(inode);
    }
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      return true;
    } else {
      return false;
    }
  } else {
    InternalPage *root_node = reinterpret_cast<InternalPage *>(old_root_node);
    if (old_root_node->GetSize() == 1) {
      page_id_t child_id = root_node->ValueAt(0);
      auto child_page = buffer_pool_manager_->FetchPage(child_id);
      BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(child_id, true);
      root_page_id_ = child_id;
      UpdateRootPageId(0);
      return true;
    } else {
      return false;
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  page_id_t root = root_page_id_;
  while (true) {
    auto *page = buffer_pool_manager_->FetchPage(root);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      INDEXITERATOR_TYPE it(root, 0, comparator_, buffer_pool_manager_);
      buffer_pool_manager_->UnpinPage(root, false);
      return it;
    } else {
      InternalPage *inode = reinterpret_cast<InternalPage *>(page->GetData());
      root = inode->ValueAt(0);
      buffer_pool_manager_->UnpinPage(inode->GetPageId(), false);
    }
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  page_id_t root = root_page_id_;
  while (true) {
    auto *page = buffer_pool_manager_->FetchPage(root);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      LeafPage *inode = reinterpret_cast<LeafPage *>(page->GetData());
      INDEXITERATOR_TYPE it(root, inode->KeyIndex(key, comparator_), comparator_, buffer_pool_manager_);
      buffer_pool_manager_->UnpinPage(root, false);
      return it;
    } else {
      InternalPage *inode = reinterpret_cast<InternalPage *>(page->GetData());
      root = inode->Lookup(key, comparator_);
      buffer_pool_manager_->UnpinPage(inode->GetPageId(), false);
    }
  }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  page_id_t root = root_page_id_;
  while (true) {
    auto *page = buffer_pool_manager_->FetchPage(root);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      INDEXITERATOR_TYPE it(root, node->GetSize() - 1, comparator_, buffer_pool_manager_);
      buffer_pool_manager_->UnpinPage(root, false);
      return (++ it);
    } else {
      InternalPage *inode = reinterpret_cast<InternalPage *>(page->GetData());
      root = inode->ValueAt(inode->GetSize() - 1);
      buffer_pool_manager_->UnpinPage(inode->GetPageId(), false);
    }
  }
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *roots_node = reinterpret_cast<IndexRootsPage *>(roots_page->GetData());
  if (insert_record) {
    roots_node->Insert(index_id_, root_page_id_);
  } else {
    roots_node->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
