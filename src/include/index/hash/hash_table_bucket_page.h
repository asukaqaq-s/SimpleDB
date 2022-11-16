#ifndef HASH_TABLE_BUCKET_PAGE_H
#define HASH_TABLE_BUCKET_PAGE_H

#include "buffer/buffer.h"
#include "record/tuple.h"


namespace SimpleDB {


/**
* @brief
* Store indexed key and and value together within bucket page. Supports
* non-unique keys.
*
* Page Header format
* ---------------------------------------------------------------------------------
* | Lsn(4) | PageType(4) | DataArray Pointer(4) | Tuple Size(4) | Value TypeID(4) |
* ---------------------------------------------------------------------------------
* 
* Bucket page format (keys are stored in order):
* ---------------------------------------------------------------------
* | PAGE HEADER(20) | IsOccurid_BitMap | IsReadable_BitMap | Tuple 1 
* ---------------------------------------------------------------------
* ---------------------------------------------
* | Tuple 2 | Tuple 3 | Tuple 4 | Tuple 5 | ...
* --------------------------------------------
* 
* Tuple format
* --------------------
* | Key | Value(RID) |
* --------------------
*/
class HashTableBucketPage : public Buffer {

    using Key = Value;


public:

    // Delete all constructor / destructor to ensure memory safety
    HashTableBucketPage() = delete;


    /**
    * init this page.
    */
    void InitHashBucketPage(int tuple_size, TypeID type);


    /**
    * Scan the bucket and collect values that have the matching key
    *
    * @return true if at least one key matched
    */
    bool GetValue(const Value &key, std::vector<RID> *result);


    /**
    * Attempts to insert a key and value in the bucket. Uses two bitmap
    * to keep track of each slot's availability.
    *
    * @param Tuple key to insert
    * @return true if inserted, false if bucket is full
    */
    bool Insert(const Value &key, const RID &value);

    
    /**
    * @return the max tuple count
    */
    int GetSize();

    /**
    * Removes a key and value.
    *
    * @return true if removed, false if not found
    */
    bool Remove(const Value &key, const RID &value);

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

public: // used by extendible hash table

    /**
    * Gets the key at an index in the bucket.
    *
    * @param bucket_idx the index in the bucket to get the key at
    * @return key at index bucket_idx of the bucket
    */
    Value KeyAt(uint32_t bucket_idx) const;


    /**
    * Gets the value at an index in the bucket.
    *
    * @param bucket_idx the index in the bucket to get the value at
    * @return value at index bucket_idx of the bucket
    */
    RID ValueAt(uint32_t bucket_idx) const;

    /**
    * Remove the KV pair at bucket_idx
    */
    void RemoveAt(uint32_t bucket_idx);


    
    void SetPageType(PageType type) {
        data_->SetPageType(type);
    }

    PageType GetPageType() const {
        return data_->GetPageType();
    }



public: // manipluate 

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

    
    
    void SetOccupied(uint32_t bucket_idx);


private:
    
    static constexpr int PAGE_LSN_OFFSET = 0;
    static constexpr int PAGE_TYPE_OFFSET = PAGE_LSN_OFFSET + sizeof(int);
    static constexpr int DATA_ARRAY_PTR_OFFSET = PAGE_TYPE_OFFSET + sizeof(int);
    static constexpr int TUPLE_SIZE_OFFSET = DATA_ARRAY_PTR_OFFSET + sizeof(int);
    static constexpr int TYPE_ID_OFFSET = TUPLE_SIZE_OFFSET + sizeof(int);
    static constexpr int PAGE_HEADER_SIZE = TYPE_ID_OFFSET + sizeof(int);
    
    void SetPageLsn(lsn_t lsn) {
        data_->SetLsn(lsn);
    }

    lsn_t GetPageLsn() const {
        return data_->GetLsn();
    }


    void SetDataArrayPtr(int n) {
        data_->SetInt(DATA_ARRAY_PTR_OFFSET, n);
    }

    int GetDataArrayPtr() const {
        return data_->GetInt(DATA_ARRAY_PTR_OFFSET);
    }

    void SetTupleSize(int size) {
        data_->SetInt(TUPLE_SIZE_OFFSET, size);
    }

    int GetTupleSize() const {
        return data_->GetInt(TUPLE_SIZE_OFFSET);
    }

    int GetMaxTupleCount() const {
        int free_space = data_->GetSize() - PAGE_HEADER_SIZE;
        int tuple_size = GetTupleSize();
        return (4 * free_space / (4 * tuple_size + 1));
    }

    int GetBitMapSize() const {
        return (GetMaxTupleCount() - 1) / 8 + 1;
    }

    TypeID GetTypeID() const {
        return static_cast<TypeID> (data_->GetInt(TYPE_ID_OFFSET));
    }

    void SetTypeID(TypeID type) {
        data_->SetInt(TYPE_ID_OFFSET, static_cast<int>(type));
    }

};

}  // namespace bustub



#endif