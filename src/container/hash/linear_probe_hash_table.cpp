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
  // // Rlock table_latch
  // table_latch_.RLock();

  // // Rlatch header page for this hash table
  // Page* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  // header_page->RLatch();
  // HashTableHeaderPage* hash_header_page = 
  //   reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());

  // // compute hash idx, block idx and bucket idx 
  // size_t hash_idx = hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * hash_header_page->NumBlocks());
  // size_t block_idx = hash_idx / BLOCK_ARRAY_SIZE;
  // size_t bucket_idx = hash_idx % BLOCK_ARRAY_SIZE;

  // // get the block page, acquire write latch 
  // Page* block_page = buffer_pool_manager_->FetchPage(hash_header_page->GetBlockPageId(block_idx));
  // block_page->WLatch();
  // auto hash_block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_all->GetData());
  
  // // keep trying to insert <key, val> into block
  // while (hash_block_page->) {

  //   // duplicate values for same key are not allowed 

  //   // resize table

  // }

  // release latches, locks and unpin 

  return false;
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
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

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
