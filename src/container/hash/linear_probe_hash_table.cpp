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
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {

      // allocate new page in buffer pool manager & acquire write latch (assume allocation always succeeds)
      Page* page = buffer_pool_manager_->NewPage(&header_page_id_);
      page->WLatch();

      // set hash table header page metadata
      HashTableHeaderPage* header_page = reinterpret_cast<HashTableHeaderPage*>(page->GetData());
      header_page->SetSize(num_buckets);
      header_page->SetPageId(header_page_id_);

      // allocate new page from buffer pool manager for num_buckets, add to hash table header page 
      for (size_t bucket = 0; bucket < num_buckets; ++bucket) {   
        page_id_t block_page_id = INVALID_PAGE_ID;
        buffer_pool_manager_->NewPage(&block_page_id);
        
        // retry if allocation failed
        if (block_page_id == INVALID_PAGE_ID) {
          --bucket;
          continue;
        }

        header_page->AddBlockPageId(block_page_id);
        buffer_pool_manager_->UnpinPage(block_page_id, false);
      }

      // unlatch hash table header page  and unpin it in buffer pool manager
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, true);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // read hash table header page 

  // compute block to access and read it 

  // keep looking until blocks are no longer occupied

    // found page

    // increment bucket

    // searched entire table

    // reached end of block, go to new block


  // unlatch and unpin pages used

  // return true of false based on result


  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Rlock table_latch
  table_latch_.RLock();

  // Rlatch header page for this hash table
  Page* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  HashTableHeaderPage* hash_header_page = 
    reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());

  // compute hash idx, block idx and bucket idx 
  size_t hash_idx = hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * hash_header_page->NumBlocks());
  size_t block_idx = hash_idx / BLOCK_ARRAY_SIZE;
  size_t bucket_idx = hash_idx % BLOCK_ARRAY_SIZE;

  // get the block page, acquire write latch 
  Page* block_page = buffer_pool_manager_->FetchPage(hash_header_page->GetBlockPageId(block_idx));
  block_page->WLatch();
  auto hash_block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
  
  // keep trying to insert <key, val> into block
  while (hash_block_page->Insert(bucket_idx, key, value) == false) {
    // duplicate values for same key are not allowed, insertion failed
    if (comparator_(hash_block_page->KeyAt(bucket_idx), key) == 0 && hash_block_page->ValueAt(bucket_idx) == value) {
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(hash_header_page->GetBlockPageId(block_idx), true);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);
      return false;
    }

    ++bucket_idx;

    // resize table when loop detected
    if (block_idx * BLOCK_ARRAY_SIZE + bucket_idx == hash_idx) {
      // release latches 
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(hash_header_page->GetBlockPageId(block_idx), true);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);

      // resize
      Resize(hash_header_page->NumBlocks() * BLOCK_ARRAY_SIZE);

      // reacquire latches
      header_page = buffer_pool_manager_->FetchPage(header_page_id_);
      header_page->RLatch();
      hash_header_page = reinterpret_cast<HashTableHeaderPage*> (header_page->GetData());

      // recompute idx
      hash_idx = hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * hash_header_page->NumBlocks());
      block_idx = hash_idx / BLOCK_ARRAY_SIZE;
      bucket_idx = hash_idx % BLOCK_ARRAY_SIZE;

      block_page = buffer_pool_manager_->FetchPage(hash_header_page->GetBlockPageId(block_idx));
      block_page->WLatch();
      hash_block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
      continue;
    }

    // done scanning current block, reset bucket_idx, block_idx and block_page
    if (bucket_idx == BLOCK_ARRAY_SIZE) {
      bucket_idx = 0;
      
      buffer_pool_manager_->UnpinPage(hash_header_page->GetBlockPageId(block_idx++), false);
      if (block_idx == hash_header_page->NumBlocks()) 
        block_idx = 0;

      block_page->WUnlatch();
      block_page = buffer_pool_manager_->FetchPage(hash_header_page->GetBlockPageId(block_idx));
      hash_block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
    }
  }

  // release latches, locks and unpin 
  block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(hash_header_page->GetBlockPageId(block_idx), true);
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // read hash table header page 

  // compute block to access and read it 

  // keep looking until blocks are no longer occupied

    // found page
      // block not readable -> this is an error, can't remove



    // increment bucket

    // searched entire table

    // reached end of block, go to new block


  // unlatch and unpin pages used

  // return true of false based on result


  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  // write lock on table latch
  table_latch_.WLock();

  // allocate new header, acquire latch, update metadata
  page_id_t old_header_page_id = header_page_id_;
  auto new_page_header = buffer_pool_manager_->NewPage(&header_page_id_); 
  new_page_header->WLatch();
  auto new_hash_page_header = reinterpret_cast<HashTableHeaderPage*>(new_page_header->GetData());

  size_t new_buckets = (2 * initial_size) / BLOCK_ARRAY_SIZE;
  new_hash_page_header->SetSize(new_buckets);
  new_hash_page_header->SetPageId(header_page_id_);

  // allocate new blocks for new header
  for (size_t bucket = 0; bucket < new_buckets; ++bucket) {   
    page_id_t block_page_id = INVALID_PAGE_ID;
    buffer_pool_manager_->NewPage(&block_page_id);
    
    // retry if allocation failed
    if (block_page_id == INVALID_PAGE_ID) {
      --bucket;
      continue;
    }

    new_hash_page_header->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
  }

  // insert <key, value> pairs into new blocks, delete olds ones
  // get old header
  Page* old_header_page = buffer_pool_manager_->FetchPage(old_header_page_id);
  old_header_page->RLatch();
  HashTableHeaderPage* old_hash_header_page = 
    reinterpret_cast<HashTableHeaderPage*>(old_header_page->GetData());
  
  // iterate through each bucket of each block
  for (size_t block = 0; block < old_hash_header_page->NumBlocks(); ++block) {
    page_id_t block_page_id = old_hash_header_page->GetBlockPageId(block);
    Page* page = buffer_pool_manager_->FetchPage(block_page_id);
    page->RLatch();
    auto page_block = reinterpret_cast
      <HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    
    for (size_t bucket_idx = 0; bucket_idx < BLOCK_ARRAY_SIZE; ++bucket_idx ) {
      if (page_block->IsReadable(bucket_idx)) {
        KeyType key = page_block->KeyAt(bucket_idx);
        ValueType val = page_block->ValueAt(bucket_idx);
        Insert(nullptr, key, val);
      }
    }

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(block_page_id, false); 
    buffer_pool_manager_->DeletePage(block_page_id);
  }


  // let go of old header
  old_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(old_header_page_id, false);
  buffer_pool_manager_->DeletePage(old_header_page_id);
  
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return 0;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
