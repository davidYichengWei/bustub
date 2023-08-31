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
  LeafPage *leaf_page = FindLeafPage(key);
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
  LeafPage *leaf_page;
  bool new_page = false;
  if (IsEmpty()) {
    new_page = true;
    // Allocate and initialize a new leaf page
    page_id_t root_page_id;
    auto *page = buffer_pool_manager_->NewPage(&root_page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
    }
    leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(root_page_id);

    // Update root_page_id_
    root_page_id_ = root_page_id;
    UpdateRootPageId(1);
  }
  else {
    leaf_page = FindLeafPage(key);
  }

  // If has enough space, just insert
  bool duplicate = false;
  if (leaf_page->InsertKeyValuePair(key, value, comparator_, duplicate)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  if (duplicate) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), new_page);
    return false;
  }

  // Split the leaf page and move half of its data
  LeafPage *new_leaf_page = SplitLeafPage(leaf_page);

  // Insert the kv pair into the correct leaf page
  if (comparator_(key, new_leaf_page->KeyAt(0)) < 0) {
    if (!leaf_page->InsertKeyValuePair(key, value, comparator_, duplicate)) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
      throw std::runtime_error("Insertion failed even after split");
    }
  }
  else {
    if (!new_leaf_page->InsertKeyValuePair(key, value, comparator_, duplicate)) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
      throw std::runtime_error("Insertion failed even after split");
    }
  }

  // Insert the first key of the new page to its parent page
  InsertIntoInternalPage(leaf_page->GetParentPageId(), leaf_page->GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId());

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }

  page_id_t current_page_id = root_page_id_;
  while (true) {
    BPlusTreePage *current_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
    if (current_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(current_page), 0, buffer_pool_manager_);
    }

    InternalPage *current_internal_page = reinterpret_cast<InternalPage *>(current_page);
    current_page_id = current_internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(current_internal_page->GetPageId(), false);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  LeafPage *leaf_page = FindLeafPage(key);
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
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) const -> LeafPage * {
  if (root_page_id_ == INVALID_PAGE_ID) return nullptr;

  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    
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

    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internal_page->ValueAt(index - 1))->GetData());
  }
  return reinterpret_cast<LeafPage *>(page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *leaf_page) -> LeafPage * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
  }
  auto *new_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  new_leaf_page->Init(new_page_id, leaf_page->GetParentPageId());

  leaf_page->SetNextPageId(new_page_id);
  leaf_page->SplitData(new_leaf_page);

  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *internal_page) -> InternalPage * {
  page_id_t new_page_id;
  auto *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
  }
  auto *new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  new_internal_page->Init(new_page_id, internal_page->GetParentPageId());

  internal_page->SplitData(new_internal_page);

  // Update parent_page_id
  for (int i = 0; i < new_internal_page->GetSize(); i++) {
    page_id_t child_page_id = new_internal_page->ValueAt(i);
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
    child_page->SetParentPageId(new_internal_page->GetParentPageId());
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }

  return new_internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalPage(page_id_t internal_page_id, page_id_t left_page_id, KeyType right_page_key, page_id_t right_page_id) {
  // Case 1: internal_page_id is INVALID_PAGE_ID 
  if (internal_page_id == INVALID_PAGE_ID) {
    // Create a new internal page, set it as the new root
    auto *page = buffer_pool_manager_->NewPage(&internal_page_id);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
    }
    auto *new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_internal_page->Init(internal_page_id);

    root_page_id_ = internal_page_id;
    UpdateRootPageId(1);

    // Insert both left_page_id and the kv pair
    new_internal_page->SetValueAt(0, left_page_id);
    new_internal_page->SetKeyValueAt(0, right_page_key, right_page_id);

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
  // Split the internal page
  InternalPage *new_internal_page = SplitInternalPage(internal_page);

  // Insert the new middle key
  InsertIntoInternalPage(internal_page->GetParentPageId(), internal_page->GetPageId(), new_internal_page->KeyAt(0), new_internal_page->GetPageId());

  // Insert the original kv pair into the correct internal page after the split
  if (comparator_(right_page_key, new_internal_page->KeyAt(0)) < 0) {
    if (!internal_page->InsertKeyValuePair(right_page_key, right_page_id, comparator_)) {
      buffer_pool_manager_->UnpinPage(internal_page_id, true);
      buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
      throw std::runtime_error("Insertion failed even after split");
    }
  }
  else {
    if (!new_internal_page->InsertKeyValuePair(right_page_key, right_page_id, comparator_)) {
      buffer_pool_manager_->UnpinPage(internal_page_id, true);
      buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
      throw std::runtime_error("Insertion failed even after split");
    }
  }

  // Unpin the 2 pages
  buffer_pool_manager_->UnpinPage(internal_page_id, true);
  buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
