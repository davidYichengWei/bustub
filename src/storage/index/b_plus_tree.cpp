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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  Page *page = FindLeafPage(key, Operation::READ, transaction);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  bool found = false;

  int left = 0, right = leaf_page->GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator_(leaf_page->KeyAt(mid), key) == 0) {
      result->emplace_back(leaf_page->ValueAt(mid));
      found = true;
      break;
    } else if (comparator_(leaf_page->KeyAt(mid), key) > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return found;
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // Create or find the leaf page to insert
  Page *page;
  LeafPage *leaf_page;
  root_latch_.lock(); // Might update root_page_id_, need to lock
  if (IsEmpty()) {
    // Allocate and initialize a new leaf page
    page_id_t root_page_id;
    page = buffer_pool_manager_->NewPage(&root_page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
    }
    leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);

    // Update root_page_id_
    root_page_id_ = root_page_id;
    UpdateRootPageId(1);

    // Latch crabbing, lock the leaf page before unlocking root_latch_
    page->WLatch();
    transaction->AddIntoPageSet(page); // Use ReleaseAllWLatch to unlock later
    root_latch_.unlock();
  }
  else {
    root_latch_.unlock(); // Need to unlock before calling FindLeafPage to avoid deadlock
    page = FindLeafPage(key, Operation::INSERT, transaction);
    leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  }

  // If insertion failed, it's a duplicate
  if (!leaf_page->InsertKeyValuePair(key, value, comparator_)) {
    ReleaseAllWLatch(transaction);
    return false;
  }

  // If leaf node doesn't reach max_size after insert, just return
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    ReleaseAllWLatch(transaction);
    return true;
  }

  // Leaf page is full, need to split the leaf page and move half of its data
  LeafPage *new_leaf_page = SplitLeafPage(leaf_page);

  // Insert the first key of the new page to its parent page
  InsertIntoInternalPage(leaf_page->GetParentPageId(), leaf_page->GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId());

  ReleaseAllWLatch(transaction);
  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);

  return true;
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
// TODO: fix insert_30 delete 1
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.lock_shared();
  if (IsEmpty()) {
    root_latch_.unlock_shared();
    return;
  }
  root_latch_.unlock_shared();

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, Operation::DELETE, transaction)->GetData());
  if (!leaf_page->RemoveKeyValuePair(key, comparator_)) {
    // LOG_INFO("Remove: key not found");
    ReleaseAllWLatch(transaction);
    return;
  }

  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    ReleaseAllWLatch(transaction);
    return;
  }

  if (leaf_page->GetPageId() == root_page_id_) {
    // Set tree empty
    if (leaf_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();

      // Add page to deleted_page_set to delete using ReleaseAllWLatch
      transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
      ReleaseAllWLatch(transaction);
      return;
    }
    ReleaseAllWLatch(transaction);
    return;
  }

  // Find the index of the leaf page in its parent page
  // This parent_page is already latched by FindLeafPage, need to Unpin manually
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId())->GetData());
  int index = -1;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    if (parent_page->ValueAt(i) == leaf_page->GetPageId()) {
      index = i;
      break;
    }
  }
  assert(index != -1);

  // First try steal from its left sibling, need to latch
  if (index != 0) {
    Page *left_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1));
    left_sibling_page->WLatch();

    LeafPage *left_sibling = reinterpret_cast<LeafPage *>(left_sibling_page->GetData());
    if (leaf_page->StealFromLeftSibling(left_sibling, comparator_)) {
      // Update key index in the parent page
      KeyType new_key = leaf_page->KeyAt(0);
      parent_page->SetKeyAt(index, new_key);

      ReleaseAllWLatch(transaction);
      left_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(), false);
  }

  // Steal from left sibling didn't work, try steal from its right sibling, need to latch
  if (index < parent_page->GetSize() - 1) {
    Page *right_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1));
    right_sibling_page->WLatch();

    LeafPage *right_sibling = reinterpret_cast<LeafPage *>(right_sibling_page->GetData());
    if (leaf_page->StealFromRightSibling(right_sibling, comparator_)) {
      // Update the key index of its right sibling in the parent page
      KeyType new_key = right_sibling->KeyAt(0);
      parent_page->SetKeyAt(index + 1, new_key);

      ReleaseAllWLatch(transaction);
      right_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    right_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling->GetPageId(), false);
  }

  // Try merge with its left sibling, call resursive helper function
  if (index != 0) {
    Page *left_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1));
    left_sibling_page->WLatch();

    LeafPage *left_sibling = reinterpret_cast<LeafPage *>(left_sibling_page->GetData());
    if (left_sibling->Merge(leaf_page)) {
      // Remove leaf page, use ReleaseAllWLatch
      page_id_t page_id_to_remove = leaf_page->GetPageId();
      transaction->AddIntoDeletedPageSet(page_id_to_remove);

      RemoveFromInternalPage(left_sibling->GetParentPageId(), page_id_to_remove, transaction);

      ReleaseAllWLatch(transaction);
      left_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(), false);
  }

  // Try merge with its right sibling, call resursive helper function
  if (index < parent_page->GetSize() - 1) {
    Page *right_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1));
    right_sibling_page->WLatch();

    LeafPage *right_sibling = reinterpret_cast<LeafPage *>(right_sibling_page->GetData());
    if (leaf_page->Merge(right_sibling)) {
      // Remove right_sibling leaf page, since it's not in transaction page set, delete it right here
      page_id_t page_id_to_remove = right_sibling->GetPageId();
      right_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id_to_remove, true);
      buffer_pool_manager_->DeletePage(page_id_to_remove);

      RemoveFromInternalPage(leaf_page->GetParentPageId(), page_id_to_remove, transaction);

      ReleaseAllWLatch(transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    right_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling->GetPageId(), false);
  }

  throw std::runtime_error("Remove failed");
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
  root_latch_.lock_shared();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_latch_.unlock_shared();
    return INDEXITERATOR_TYPE();
  }

  Page *prev_page = nullptr;
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->RLatch();
  root_latch_.unlock_shared();
  while (true) {
    BPlusTreePage *current_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (current_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(current_page), 0, buffer_pool_manager_);
    }

    InternalPage *current_internal_page = reinterpret_cast<InternalPage *>(current_page);
    prev_page = page;
    page = buffer_pool_manager_->FetchPage(current_internal_page->ValueAt(0));
    page->RLatch();
    prev_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, Operation::READ, nullptr)->GetData());
  if (leaf_page == nullptr) {
    return INDEXITERATOR_TYPE();
  }

  int index = leaf_page->GetSize() - 1;
  int left = 0, right = leaf_page->GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator_(leaf_page->KeyAt(mid), key) >= 0) {
      index = mid;
      right = mid - 1;
    }
    else {
      left = mid + 1;
    }
  }

  return INDEXITERATOR_TYPE(leaf_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation operation, Transaction *transaction) -> Page * {
  if (operation == Operation::READ) {
    root_latch_.lock_shared();
  }
  else {
    root_latch_.lock();
    transaction->AddIntoPageSet(nullptr); // Add nullptr to indicate that root_latch_ is acquired
  }

  if (root_page_id_ == INVALID_PAGE_ID) {
    if (operation == Operation::READ) {
      root_latch_.unlock_shared();
    }
    else {
      root_latch_.unlock();
    }
    return nullptr;
  }

  Page *prev_page = nullptr;
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (operation == Operation::READ) {
    page->RLatch();
    root_latch_.unlock_shared();
  }
  else {
    page->WLatch();
    transaction->AddIntoPageSet(page);
  }

  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(tree_page);

    // Find the index of the first key that is greater than the given key
    int index = internal_page->GetSize();
    int left = 1, right = internal_page->GetSize() - 1;
    while (left <= right) {
      int mid = (left + right) / 2;
      if (comparator_(internal_page->KeyAt(mid), key) > 0) {
        index = mid;
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    prev_page = page;
    page = buffer_pool_manager_->FetchPage(internal_page->ValueAt(index - 1));
    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // Latch crabbing
    if (operation == Operation::READ) {
      page->RLatch();
      prev_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
    }
    else if (operation == Operation::INSERT) {
      page->WLatch();
      if (IsSafe(tree_page, Operation::INSERT)) {
        ReleaseAllWLatch(transaction);
      }
      transaction->AddIntoPageSet(page);
    }
    else if (operation == Operation::DELETE) {
      page->WLatch();
      if (IsSafe(tree_page, Operation::DELETE)) {
        ReleaseAllWLatch(transaction);
      }
      transaction->AddIntoPageSet(page);
    }
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *leaf_page) -> LeafPage * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
  }
  auto *new_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  new_leaf_page->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());

  leaf_page->SetNextPageId(new_page_id);
  leaf_page->SplitData(new_leaf_page);

  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *internal_page, 
                                       std::vector<std::pair<KeyType, page_id_t>> kv_pairs_after_insert) -> InternalPage * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
  }
  auto *new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  new_internal_page->Init(new_page_id, internal_page->GetParentPageId(), internal_max_size_);

  // Split data
  int mid_index = internal_page->GetMinSize();
  for (int i = 0; i < mid_index; i++) {
    internal_page->SetKeyValueAt(i, kv_pairs_after_insert[i].first, kv_pairs_after_insert[i].second);
  }
  internal_page->SetSize(mid_index);
  for (size_t i = mid_index, j = 0; i < kv_pairs_after_insert.size(); i++, j++) {
    new_internal_page->SetKeyValueAt(j, kv_pairs_after_insert[i].first, kv_pairs_after_insert[i].second);
  }
  new_internal_page->SetSize(kv_pairs_after_insert.size() - mid_index);

  // Update parent_page_id of children of new internal page
  for (int i = 0; i < new_internal_page->GetSize(); i++) {
    page_id_t child_page_id = new_internal_page->ValueAt(i);
    auto *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
    child_page->SetParentPageId(new_internal_page->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }

  return new_internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalPage(page_id_t internal_page_id, page_id_t left_page_id, KeyType right_page_key, page_id_t right_page_id) {
  // Case 1: internal_page_id is INVALID_PAGE_ID
  if (internal_page_id == INVALID_PAGE_ID) {
    // Create a new internal page, set it as the new root
    // Note: No need to acquire root_latch_ here, since we would have already acquired it
    auto *page = buffer_pool_manager_->NewPage(&internal_page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
    }
    auto *new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_internal_page->Init(internal_page_id, INVALID_PAGE_ID, internal_max_size_);

    root_page_id_ = internal_page_id;
    UpdateRootPageId();

    // Insert both left_page_id and the kv pair
    new_internal_page->SetValueAt(0, left_page_id);
    new_internal_page->SetKeyValueAt(1, right_page_key, right_page_id);
    new_internal_page->IncreaseSize(2);

    // Update the parent_page_id field of both left page and right page
    auto *left_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_page_id)->GetData());
    auto *right_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_page_id)->GetData());
    left_page->SetParentPageId(internal_page_id);
    right_page->SetParentPageId(internal_page_id);

    // Unpin all 3 pages
    buffer_pool_manager_->UnpinPage(internal_page_id, true);
    buffer_pool_manager_->UnpinPage(left_page_id, true);
    buffer_pool_manager_->UnpinPage(right_page_id, true);
    return;
  }

  // Case 2: page with internal_page_id is not full
  InternalPage *internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page_id)->GetData());
  if (internal_page->InsertKeyValuePair(right_page_key, right_page_id, comparator_)) {
    buffer_pool_manager_->UnpinPage(internal_page_id, true);
    return;
  }

  // Case 3: page with internal_page_id is full
  // Store kv pairs after insertion in a vector
  std::vector<std::pair<KeyType, page_id_t>> kv_pairs_after_insert = internal_page->InsertToTmpVector(right_page_key, right_page_id, comparator_);
  // Create a new internal page, split data in vec into 2 pages
  InternalPage *new_internal_page = SplitInternalPage(internal_page, kv_pairs_after_insert);

  // Insert the new middle key
  InsertIntoInternalPage(internal_page->GetParentPageId(), internal_page->GetPageId(), new_internal_page->KeyAt(0), new_internal_page->GetPageId());

  // Unpin the 2 pages
  buffer_pool_manager_->UnpinPage(internal_page_id, true);
  buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
}

// Don't use ReleaseAllWLatch in this function, only add page to delete to transaction
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromInternalPage(page_id_t internal_page_id, page_id_t page_id_to_remove, Transaction *transaction) {
  if (internal_page_id == INVALID_PAGE_ID) throw std::runtime_error("Remove from invalid internal page");
  // This internal_page is already latched by FindLeafPage, need to manually Unpin in this function
  InternalPage *internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page_id)->GetData());

  if (!internal_page->RemoveKeyValuePair(page_id_to_remove)) {
    throw std::runtime_error("Failed to remove kv pair from internal page");
  }

  // Case 1: size >= min_size after removal
  if (internal_page->GetSize() >= internal_page->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(internal_page_id, true);
    return;
  }

  // Case 2: size < min_size after removal, page is root page
  // root_latch_ already acquired
  if (internal_page_id == root_page_id_) {
    // size > 1: unpin page and return
    if (internal_page->GetSize() > 1) {
      buffer_pool_manager_->UnpinPage(internal_page_id, true);
      return;
    }

    // size == 1, set its child as new root, delete page, unpin and return
    BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(0))->GetData());
    child_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_page->GetPageId();
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

    buffer_pool_manager_->UnpinPage(internal_page_id, true);
    transaction->AddIntoDeletedPageSet(internal_page_id); // Use ReleaseAllWLatch to delete page
    return;
  }

  // Case 3: size < min_size after removal, page is not root page
  // Find the index of the internal page in its parent page
  // parent_page already latched, but need to Unpin it in this function
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page->GetParentPageId())->GetData());
  int index = -1;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    if (parent_page->ValueAt(i) == internal_page->GetPageId()) {
      index = i;
      break;
    }
  }
  assert(index != -1);

  // First try steal from its left sibling, need to latch
  if (index != 0) {
    Page *left_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1));
    left_sibling_page->WLatch();

    InternalPage *left_sibling = reinterpret_cast<InternalPage *>(left_sibling_page->GetData());
    if (internal_page->StealFromLeftSibling(left_sibling, comparator_)) {
      // Update key index in the parent page
      KeyType new_key = internal_page->KeyAt(0);
      parent_page->SetKeyAt(index, new_key);

      // Update parent_page_id of the child page stolen,
      // no latch needed since its parent is latched
      BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(0))->GetData());
      child_page->SetParentPageId(internal_page->GetPageId());

      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
      left_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), false);
  }

  // Steal from left sibling didn't work, try steal from its right sibling
  if (index < parent_page->GetSize() - 1) {
    Page *right_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1));
    right_sibling_page->WLatch();

    InternalPage *right_sibling = reinterpret_cast<InternalPage *>(right_sibling_page->GetData());
    if (internal_page->StealFromRightSibling(right_sibling, comparator_)) {
      // Update the key index of its right sibling in the parent page
      KeyType new_key = right_sibling->KeyAt(0);
      parent_page->SetKeyAt(index + 1, new_key);

      // Update parent_page_id of the child page stolen, no latch needed
      BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->
        FetchPage(internal_page->ValueAt(internal_page->GetSize() - 1))->GetData());
      child_page->SetParentPageId(internal_page->GetPageId());

      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
      right_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    right_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
  }

  // Try merge with its left sibling, call resursive helper function
  if (index != 0) {
    Page *left_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1));
    left_sibling_page->WLatch();

    InternalPage *left_sibling = reinterpret_cast<InternalPage *>(left_sibling_page->GetData());
    int old_size = left_sibling->GetSize();
    if (left_sibling->Merge(internal_page)) { // Merge internal_page into left_sibling
      page_id_t page_id_to_remove = internal_page->GetPageId();
      buffer_pool_manager_->UnpinPage(page_id_to_remove, true);
      transaction->AddIntoDeletedPageSet(page_id_to_remove); // Use ReleaseAllWLatch to delete page

      RemoveFromInternalPage(left_sibling->GetParentPageId(), page_id_to_remove, transaction);

      // Update parent_page_id of the child pages merged,
      // no need to latch since its parent is latched
      for (int i = old_size; i < left_sibling->GetSize(); i++) {
        BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_sibling->ValueAt(i))->GetData());
        child_page->SetParentPageId(left_sibling->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      }

      left_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sibling->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    left_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), false);
  }

  // Try merge with its right sibling, call resursive helper function
  if (index < parent_page->GetSize() - 1) {
    Page *right_sibling_page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1));
    right_sibling_page->WLatch();

    InternalPage *right_sibling = reinterpret_cast<InternalPage *>(right_sibling_page->GetData());
    int old_size = internal_page->GetSize();
    if (internal_page->Merge(right_sibling)) { // Merge right_sibling into internal_page
      // Here we are deleting right_sibling, which is not in the transaction's page set, so delete it right here
      page_id_t page_id_to_remove = right_sibling->GetPageId();
      right_sibling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id_to_remove, true);
      buffer_pool_manager_->DeletePage(page_id_to_remove);

      RemoveFromInternalPage(internal_page->GetParentPageId(), page_id_to_remove, transaction);

      // Update parent_page_id of the child pages merged
      for (int i = old_size; i < internal_page->GetSize(); i++) {
        BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(i))->GetData());
        child_page->SetParentPageId(internal_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      }

      buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      return;
    }
    right_sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
  }

  throw std::runtime_error("Remove from internal page failed");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllWLatch(Transaction *transaction) {
  if (transaction == nullptr) return;

  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page == nullptr) {
      root_latch_.unlock();
    }
    else {
      page->WUnlatch();
      page_id_t page_id = page->GetPageId();
      buffer_pool_manager_->UnpinPage(page_id, true);

      // Delete the page if needed
      auto deleted_page_set = *(transaction->GetDeletedPageSet());
      if (deleted_page_set.find(page_id) != deleted_page_set.end()) {
        buffer_pool_manager_->DeletePage(page_id);
        deleted_page_set.erase(page_id);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *page, Operation operation) -> bool {
  if (operation == Operation::READ) {
    return true;
  }
  if (operation == Operation::INSERT) {
    if (page->IsLeafPage()) {
      return page->GetSize() < page->GetMaxSize() - 1;
    }
    else {
      return page->GetSize() < page->GetMaxSize();
    }
  }
  if (operation == Operation::DELETE) {
    if (page->IsRootPage()) {
      if (page->IsLeafPage()) {
        return page->GetSize() > 0;
      }
      return page->GetSize() > 1;
    }
    else {
      return page->GetSize() > page->GetMinSize();
    }
  }
  return false;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
