#ifndef BPLUS_TREE_BUCKET_PAGE_H
#define BPLUS_TREE_BUCKET_PAGE_H


#include "buffer/buffer.h"
#include "config/config.h"
#include "config/type.h"
#include "record/rid.h"
#include "concurrency/transaction.h"


namespace SimpleDB {

/**
* @brief
* Header format:
* -------------------------------------------
* | LSN(4) | PageType(4) | NextBucketNum(4) |
* -------------------------------------------
* BucketPage format:
* --------------------------------------------------------------------
* | Header(12) | IsReadable BitMap | IsOccupied BitMap | RID ARRAY   |
* --------------------------------------------------------------------
*/
class BPlusTreeBucketPage {


    /**
    * init this page
    */
    void Init();

// in BPlusBucketPage, since all rid are belong to the same key,
// we don't need to pass key as a param


    /**
    * Scan the bucket and collect values that have the matching key
    * 
    * @return true if at least one key matched
    */
    bool GetValue(std::vector<RID> *result);


    /**
    * Attempts to insert a rid into the bucket. Uses two bitmap
    * to keep track of each slot's availability.
    * 
    * @param Tuple key to insert
    * @return true if inserted, false if bucket is full
    */
    bool Insert(const RID &value);

    
    /**
    * @return the max tuple count
    */
    int GetSize();


    /**
    * Removes a rid.
    *
    * @return true if removed, false if not found
    */
    bool Remove(const RID &value);


    /**
    * check if this page is full and can't insert again
    */
    bool IsFull();

    /**
    * check if this page is empty
    */
    bool IsEmpty();

    /**
    * Prints the bucket's occupancy information
    */
    void PrintBucket();


public: // used by Bplus tree



    /**
    * Gets the rid at an index in the bucket.
    *
    * @param bucket_idx the index in the bucket to get the rid at
    * @return rid at index bucket_idx of the bucket
    */
    RID RIDAt(uint32_t bucket_idx) const;


    /**
    * Remove rid at bucket_idx
    */
    void RemoveAt(uint32_t bucket_idx);

    
    void SetPageType(PageType type) {
        page_type_ = type;
    }

    PageType GetPageType() const {
        return page_type_;
    }



public: // manipluate bit map

    /**
    * Returns whether or not an index is readable (valid key/value pair)
    *
    * @param bucket_idx index to lookup
    * @return true if the index is readable, false otherwise
    */
    bool IsReadable(uint32_t bucket_idx);


    /**
    * SetReadable - Updates the bitmap to indicate that the entry at
    * bucket_idx is readable.
    *
    * @param bucket_idx the index to update
    */
    void SetReadable(uint32_t bucket_idx);


    /**
    * SetReadable - Updates the bitmap to indicate that the entry at
    * bucket_idx is readable.
    *
    * @param bucket_idx the index to update
    */
    void SetUnReadable(uint32_t bucket_idx);

    
    /**
    * Returns whether or not an index is occupied
    *
    * @param bucket_idx index to lookup
    * @return true if the index is occupied, false otherwise
    */
    bool IsOccupied(uint32_t bucket_idx);

    
    /**
    * set this bucket has been occupied
    */
    void SetOccupied(uint32_t bucket_idx);


private:

    // since the size of rid is constant, we don't need to store tuplesize 
    // and bitmap size. they are constant value also.
    
    static constexpr int FREE_SPACE = SIMPLEDB_BLOCK_SIZE - Page::PAGE_HEADER_SIZE;
    static constexpr int KVPAIR_SIZE = sizeof(RID);
    static constexpr int BPLUS_TREE_BUCKET_MAX_SIZE = (4 * FREE_SPACE / (4 * KVPAIR_SIZE + 1));
    static constexpr int BPLUS_TREE_BIT_MAP_SIZE = (BPLUS_TREE_BUCKET_MAX_SIZE - 1) / 8 + 1;
    static constexpr int PAGE_HEADER_SIZE = Page::PAGE_HEADER_SIZE;


    lsn_t lsn_;

    PageType page_type_;

    int next_bucket_num_;

    char readable_[BPLUS_TREE_BIT_MAP_SIZE];
    
    char occupied_[BPLUS_TREE_BIT_MAP_SIZE];

    RID data_[0];
};

} // namespace SimpleDB

#endif