//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)),
      num_buckets(num_buckets) {
  header_page_id_ = INVALID_PAGE_ID;
  Page *header_page = buffer_pool_manager->NewPage(&header_page_id_);
  HashTableHeaderPage *ht_header_page = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  size_t slots_per_ht_block = (BLOCK_ARRAY_SIZE - 1) / 8 + 1;
  this->num_blocks = num_buckets / slots_per_ht_block;
  for (size_t i = 0; i < num_blocks; i++) {
    page_id_t block_pageid;
    buffer_pool_manager->NewPage(&block_pageid);
    ht_header_page->AddBlockPageId(block_pageid);
  }
  ht_header_page->SetPageId(header_page_id_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableHeaderPage *HASH_TABLE_TYPE::GetHeaderPage() {
  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  return reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetNumberOfBlockPages() {
  HashTableHeaderPage *header = GetHeaderPage();
  return header->NumBlocks();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableBlockPage<KeyType, ValueType, KeyComparator> *HASH_TABLE_TYPE::GetHashTableBlockPage(size_t block_number) {
  HashTableHeaderPage *header = GetHeaderPage();
  size_t page_id = header->GetBlockPageId(block_number);
  Page *block_page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(block_page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  size_t index = hash_fn_.GetHash(key) % num_buckets;  // index in the hash table
  size_t start_block = index % num_blocks;
  size_t initial_block = start_block;
  bool found_empty_slot = false;
  while (true) {
    auto block_page = GetHashTableBlockPage(start_block);
    // TODO: Confirm
    constexpr size_t slots_in_page = (BLOCK_ARRAY_SIZE - 1) / 8 + 1;
    for (size_t i = 0; i < slots_in_page; i++) {
      if (block_page->IsReadable(i)) {
        if (!comparator_(block_page->KeyAt(i), key)) {
          result->push_back(block_page->ValueAt(i));
        }
      } else if (!block_page->IsOccupied(i)) {  // Empty slot
        found_empty_slot = true;
        break;
      }
    }
    start_block += 1;
    start_block %= num_blocks;
    if (found_empty_slot) break;
    if (start_block == initial_block) {  // O(n)
      break;
    }
  }
  return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // TODO: When to return false?
  size_t index = hash_fn_.GetHash(key) % num_buckets;  // index in the hash table
  size_t num_blocks = GetNumberOfBlockPages();
  size_t start_block = index % num_blocks;
  size_t initial_block = start_block;
  while (true) {
    auto block_page = GetHashTableBlockPage(start_block);
    // TODO: Confirm
    constexpr size_t slots_in_page = (BLOCK_ARRAY_SIZE - 1) / 8 + 1;
    for (size_t i = 0; i < slots_in_page; i++) {
      if (block_page->IsReadable(i)) {
        if (!comparator_(block_page->KeyAt(i), key)) {
          if (block_page->ValueAt(i) == value) {  // duplicate (key, value)
            return false;
          }
        }
      } else if (!block_page->IsOccupied(i)) {  // Empty slot
        block_page->Insert(i, key, value);
        return true;
      }
    }
    start_block += 1;
    start_block %= num_blocks;
    if (start_block == initial_block) {  // O(n)
      break;
    }
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  size_t index = hash_fn_.GetHash(key) % num_buckets;  // index in the hash table
  size_t start_block = index % num_blocks;
  size_t initial_block = start_block;
  while (true) {
    auto block_page = GetHashTableBlockPage(start_block);
    // TODO: Confirm
    constexpr size_t slots_in_page = (BLOCK_ARRAY_SIZE - 1) / 8 + 1;
    for (size_t i = 0; i < slots_in_page; i++) {
      if (block_page->IsReadable(i)) {
        if (!comparator_(block_page->KeyAt(i), key)) {
          if (block_page->ValueAt(i) == value) {
            block_page->Remove(i);
            return true;
          }
        }
      } else if (!block_page->IsOccupied(i)) {  // Empty slot
        return false;
      }
    }
    start_block += 1;
    start_block %= num_blocks;
    if (start_block == initial_block) {  // O(n)
      break;
    }
  }
  return false;  // No such item to delete
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  // TODO: Reinsert all elements
  // TODO: Update num_blocks
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return num_blocks;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
