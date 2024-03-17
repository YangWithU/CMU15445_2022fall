#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return this->root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/

// function to get page[key] from B+ tree by trversing the tree
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation operation, Transaction *transaction, bool leftmost,
                                  bool rightmost) -> Page * {
  auto *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root_tree_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());

  if (operation == Operation::SEARCH) {
    root_page_latch_.RUnlock();
    root_page->RLatch();
  } else {
    root_page->WLatch();
    if (operation == Operation::DELETE && root_tree_page->GetSize() > 2) {
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && root_tree_page->IsLeafPage() &&
        root_tree_page->GetSize() < root_tree_page->GetMaxSize() - 1) {
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && !root_tree_page->IsLeafPage() &&
        root_tree_page->GetSize() < root_tree_page->GetMaxSize()) {
      ReleaseLatchFromQueue(transaction);
    }
  }

  while (!root_tree_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(root_tree_page);

    page_id_t lookup_page_id{};
    if (leftmost) {
      lookup_page_id = internal_page->ValueAt(0);
    } else if (rightmost) {
      lookup_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    } else {
      // now find value corresponds to key
      lookup_page_id = internal_page->FindValueOnInternalPage(key, comparator_);
    }

    auto child_page = buffer_pool_manager_->FetchPage(lookup_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::SEARCH) {
      child_page->RLatch();
      root_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(root_page->GetPageId(), false);
    } else if (operation == Operation::INSERT) {
      child_page->WLatch();
      transaction->AddIntoPageSet(root_page);

      // if child node is safe, release all locks on ancestors
      if (child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize() - 1) {
        ReleaseLatchFromQueue(transaction);
      }
      if (!child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    } else if (operation == Operation::DELETE) {
      child_page->WLatch();
      transaction->AddIntoPageSet(root_page);

      // if child node is safe, release all locks on ancestors
      if (child_node->GetSize() > child_node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    }

    root_page = child_page;
    root_tree_page = child_node;
  }

  return root_page;
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_latch_.RLock();
  // find leaf node first
  Page *page = FindLeafPage(key, Operation::SEARCH, transaction);

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType lookup_value{};
  bool found = leaf_page->FindValueOnLeaf(key, &lookup_value, comparator_);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (!found) {
    return false;
  }

  result->push_back(lookup_value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

// to init a whole new B+ tree
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InitNewTree(const KeyType &key, const ValueType &value) -> void {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);

  BUSTUB_ASSERT(page != nullptr, "buffer_pool_manager unable init new page(NewPage: false)");

  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  UpdateRootPageId(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *src_leaf, BPlusTreePage *dst_leaf, const KeyType &dst_start_key,
                                      Transaction *transaction) -> void {
  if (src_leaf->IsRootPage()) {
    auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "In InsertIntoParent(): buffer_pool_manager_->NewPage failed");

    auto *n_root_page = reinterpret_cast<InternalPage *>(page->GetData());
    n_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    // param: old value, new key, new value
    n_root_page->InitNewRoot(src_leaf->GetPageId(), dst_start_key, dst_leaf->GetPageId());

    src_leaf->SetParentPageId(n_root_page->GetPageId());
    dst_leaf->SetParentPageId(n_root_page->GetPageId());

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    UpdateRootPageId(0);

    ReleaseLatchFromQueue(transaction);
  } else {
    auto *page = buffer_pool_manager_->FetchPage(src_leaf->GetParentPageId());
    auto *parent_page = reinterpret_cast<InternalPage *>(page->GetData());

    // if parent not full
    if (parent_page->GetSize() < internal_max_size_) {
      // just directly insert data
      parent_page->InsertDataToParentPage(src_leaf->GetPageId(), dst_start_key, dst_leaf->GetPageId());

      ReleaseLatchFromQueue(transaction);

      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    } else {
      /* parent page full, do another split */
      // 加一是给dst page的key留的
      // 新的 page 作为临时page, 专门用来split
      auto *raw_data = new char[INTERNAL_PAGE_HEADER_SIZE + (parent_page->GetSize() + 1) * sizeof(MappingType)];

      // copy N size from src to dst
      //           dst       src               size
      std::memcpy(raw_data, page->GetData(),
                  (parent_page->GetSize()) * sizeof(MappingType) + INTERNAL_PAGE_HEADER_SIZE);

      auto *tmp_parent_page = reinterpret_cast<InternalPage *>(raw_data);
      tmp_parent_page->InsertDataToParentPage(src_leaf->GetPageId(), dst_start_key, dst_leaf->GetPageId());

      // when insert completed, start split
      auto *splitted_parent_page = SplitBptreePage(tmp_parent_page);

      // 把修改后的数据拷回去
      std::memcpy(page->GetData(), raw_data,
                  (tmp_parent_page->GetMinSize()) * sizeof(MappingType) + INTERNAL_PAGE_HEADER_SIZE);

      // 递归插新parent
      auto nxt_key = splitted_parent_page->KeyAt(0);
      InsertIntoParent(parent_page, splitted_parent_page, nxt_key, transaction);

      // 递归结束:回收资源
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(splitted_parent_page->GetPageId(), true);
      delete[] raw_data;
    }
  }
}

// returns splitted new page
INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
auto BPLUSTREE_TYPE::SplitBptreePage(PageType *page_to_split) -> PageType * {
  page_id_t page_id{};
  auto n_bpm_page = buffer_pool_manager_->NewPage(&page_id);

  BUSTUB_ASSERT(n_bpm_page != nullptr, "In SplitLeaf(): buffer_pool_manager_->NewPage failed");

  // set page type first
  auto *n_typed_page = reinterpret_cast<PageType *>(n_bpm_page->GetData());
  n_typed_page->SetPageType(page_to_split->GetPagetype());

  if (page_to_split->IsLeafPage()) {
    // 1.cast src type
    auto *leaf_page = reinterpret_cast<LeafPage *>(page_to_split);

    // 2.make new leaf page
    auto n_leaf_page = reinterpret_cast<LeafPage *>(n_typed_page);

    // when getting page_id, remember to call un-casted page
    // In this case, we call n_bpm_page->GetPageId()
    n_leaf_page->Init(n_bpm_page->GetPageId(), page_to_split->GetParentPageId(), leaf_max_size_);

    // 3.move src array_ to dst

    // move max - min sized data from leaf_page_to_split to n_leaf_page
    // src: leaf_page_to_split, dst: n_leaf_page
    leaf_page->MoveHalfTo(n_leaf_page);
  } else {
    auto *internal_page = reinterpret_cast<InternalPage *>(page_to_split);

    auto *n_internal_page = reinterpret_cast<InternalPage *>(n_typed_page);
    n_internal_page->Init(n_bpm_page->GetPageId(), page_to_split->GetParentPageId(), internal_max_size_);

    internal_page->MoveHalfTo(n_internal_page, buffer_pool_manager_);
  }

  return n_typed_page;
}

// case when root already exist
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  auto *page = FindLeafPage(key, Operation::INSERT, transaction);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  auto bf = leaf_page->GetSize();
  auto aft = leaf_page->Insert(key, value, comparator_);

  // duplicate key
  if (bf == aft) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  // leaf not full, insert complete
  if (aft < leaf_max_size_) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    // insert completed, array modified, write page back to disk
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }

  /* now means that leaf is full, split */
  // begin split
  auto n_leaf_page = SplitBptreePage(leaf_page);

  // re-order leaf page connection, similar to linked-list
  // new take over old next_page
  n_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  // old next_page = new_page
  leaf_page->SetNextPageId(n_leaf_page->GetPageId());

  auto n_arr_head_key = n_leaf_page->KeyAt(0);
  InsertIntoParent(leaf_page, n_leaf_page, n_arr_head_key, transaction);
  // end split

  page->WUnlatch();

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(n_leaf_page->GetPageId(), true);
  return true;
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_latch_.WLock();
  // 将根加入transaction队列
  // root_page_latch_是nullptr
  transaction->AddIntoPageSet(nullptr);

  // judge a new tree in the first place
  if (this->IsEmpty()) {
    InitNewTree(key, value);
    ReleaseLatchFromQueue(transaction);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  if (IsEmpty()) {
    ReleaseLatchFromQueue(transaction);
    return;
  }

  auto *page = FindLeafPage(key, Operation::DELETE, transaction);
  auto *leaf_page_to_del = reinterpret_cast<LeafPage *>(page->GetData());

  // if not found (size unchanged), return
  if (leaf_page_to_del->GetSize() == leaf_page_to_del->RemoveArrayRecord(key, comparator_)) {
    ReleaseLatchFromQueue(transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }

  // after deletion
  auto to_delete_leaf = CoalesceOrRedistribute(leaf_page_to_del, transaction);
  page->WUnlatch();

  if (to_delete_leaf) {
    transaction->AddIntoDeletedPageSet(leaf_page_to_del->GetPageId());
  }

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [&](const page_id_t page_id) { buffer_pool_manager_->DeletePage(page_id); });

  transaction->GetDeletedPageSet()->clear();
}

/*
 * 清空 transaction 的pageset，解锁共享锁
 * 意思就是释放此前过程加入pageset的所有page的锁
 * 对B+树而言就是ancestor
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    auto *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      this->root_page_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

/*
 * 尝试合并或重分发
 * false:不需要删除
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(PageType *page_to_del, Transaction *transaction) -> bool {
  if (page_to_del->IsRootPage()) {
    auto res = AdjustRoot(page_to_del);
    ReleaseLatchFromQueue(transaction);
    return res;
  }

  // page >= minsize 不用合并
  if (page_to_del->GetSize() >= page_to_del->GetMinSize()) {
    ReleaseLatchFromQueue(transaction);
    return false;  // 不必删当前page
  }

  auto *parent_page = buffer_pool_manager_->FetchPage(page_to_del->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto par_page_idx = parent_node->GetValueIndex(page_to_del->GetPageId());

  // 操作左边的兄弟
  if (par_page_idx > 0) {
    auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(par_page_idx - 1));

    sibling_page->WLatch();
    auto *sibling_node = reinterpret_cast<PageType *>(sibling_page->GetData());

    // size > minsize 不合并, redistribute
    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      Redistribute(sibling_node, page_to_del, parent_node, par_page_idx, true);

      ReleaseLatchFromQueue(transaction);

      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return false;
    }

    // now colaesce
    auto to_delete_parent = Coalesce(sibling_node, page_to_del, parent_node, par_page_idx, transaction);
    if (to_delete_parent) {
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;
  }

  // par_page_idx == 0 && !唯一元素
  // 此时只能操作右边的兄弟
  if (par_page_idx != parent_node->GetSize() - 1) {
    auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(par_page_idx + 1));
    sibling_page->WLatch();
    auto *sibling_node = reinterpret_cast<PageType *>(sibling_page->GetData());

    if (sibling_node->GetSize() > sibling_node->GetMinSize()) {
      // only redis
      Redistribute(sibling_node, page_to_del, parent_node, par_page_idx, false);

      ReleaseLatchFromQueue(transaction);

      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return false;
    }

    auto sibling_idx = parent_node->GetValueIndex(sibling_node->GetPageId());
    auto to_del_par_node = Coalesce(page_to_del, sibling_node, parent_node, sibling_idx, transaction);

    transaction->AddIntoDeletedPageSet(sibling_node->GetPageId());

    if (to_del_par_node) {
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }

  return false;
}

/*
 * 用于删根时调整根，调了回true，没调回false
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root) -> bool {
  // 有子节点且只有一个
  if (!old_root->IsLeafPage() && old_root->GetSize() == 1) {
    auto *root_page = reinterpret_cast<InternalPage *>(old_root);
    auto *only_child_page = buffer_pool_manager_->FetchPage(root_page->ValueAt(0));
    auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(only_child_page->GetData());

    child_tree_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_tree_page->GetPageId();
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(only_child_page->GetPageId(), true);
    return true;
  }

  // 无子节点
  if (old_root->IsLeafPage() && old_root->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
auto BPLUSTREE_TYPE::Coalesce(PageType *node_to_coalesce, PageType *sibling_node, InternalPage *parent,
                              int par_page_idx, Transaction *transaction) -> bool {
  auto middle_key = parent->KeyAt(par_page_idx);

  if (node_to_coalesce->IsLeafPage()) {
    auto *sib_leaf_page = reinterpret_cast<LeafPage *>(sibling_node);
    auto *page_to_col = reinterpret_cast<LeafPage *>(node_to_coalesce);

    sib_leaf_page->MoveAllTo(page_to_col);
  } else {
    auto *sib_intern_page = reinterpret_cast<InternalPage *>(sibling_node);
    auto *page_to_col = reinterpret_cast<InternalPage *>(node_to_coalesce);

    sib_intern_page->MoveAllTo(page_to_col, middle_key, buffer_pool_manager_);
  }

  parent->FillEmptyAftCoal(par_page_idx);
  return CoalesceOrRedistribute(parent, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
void BPLUSTREE_TYPE::Redistribute(PageType *sibling_node, PageType *node_to_redist, InternalPage *parent,
                                  int par_page_idx, bool from_prev) {
  if (node_to_redist->IsLeafPage()) {
    auto *leaf_page = reinterpret_cast<LeafPage *>(node_to_redist);
    auto *sibling_page = reinterpret_cast<LeafPage *>(sibling_node);

    //
    if (!from_prev) {
      // move first page of sibling_node to end of node_to_redist
      sibling_page->MoveFirstToEndOf(leaf_page);
      parent->SetKeyAt(par_page_idx + 1, sibling_page->KeyAt(0));
    } else {
      sibling_page->MoveLastToFrontOf(leaf_page);
      parent->SetKeyAt(par_page_idx, leaf_page->KeyAt(0));
    }
    // internal page
  } else {
    auto *intern_page = reinterpret_cast<InternalPage *>(node_to_redist);
    auto *sibling_page = reinterpret_cast<InternalPage *>(sibling_node);

    if (!from_prev) {
      sibling_page->MoveFirstToEndOf(intern_page, parent->KeyAt(par_page_idx + 1), buffer_pool_manager_);
      parent->SetKeyAt(par_page_idx + 1, sibling_page->KeyAt(0));
    } else {
      sibling_page->MoveLastToFrontOf(intern_page, parent->KeyAt(par_page_idx), buffer_pool_manager_);
      parent->SetKeyAt(par_page_idx, intern_page->KeyAt(0));
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // judge if tree exists
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }

  root_page_latch_.RLock();

  auto *page = FindLeafPage(KeyType(), Operation::SEARCH, nullptr, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // judge if tree exists
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }

  root_page_latch_.RLock();

  auto *page = FindLeafPage(key, Operation::SEARCH);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  auto idx = leaf_page->GetIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // judge if tree exists
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }

  root_page_latch_.RLock();

  auto *page = FindLeafPage(KeyType(), Operation::SEARCH, nullptr, false, true);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
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
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
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
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
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
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
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
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
