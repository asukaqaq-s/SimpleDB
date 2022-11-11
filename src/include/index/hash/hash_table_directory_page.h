#ifndef HASH_TABLE_DIRECTORY_PAGE_H
#define HASH_TABLE_DIRECTORY_PAGE_H

#include "buffer/buffer.h"
#include "config/config.h"
#include "config/type.h"



namespace SimpleDB {

/**
 *
 * Directory Page for extendible hash table.
 *
 * Directory format (size in byte):
 * --------------------------------------------------------------------------------------------
 * | LSN (4) | BlockNum(4) | GlobalDepth(4) | LocalDepths(512) | BucketBlockNums(2048) | Free(1524)
 * --------------------------------------------------------------------------------------------
 */
class HashTableDirectoryPage {

using block_num_t = int;

public:
    
    /**
    * @return the Block number of this page
    */
    int GetBlockNum() const;

    /**
    * @brief Sets the Block number of this page
    *
    * @param block_num the page id to which to set the block_num_ field
    */
    void SetBlockNum(int block_num) {}

    /**
    * @return the lsn of this page
    */
    lsn_t GetLSN() const;

    /**
    * Sets the LSN of this page
    *
    * @param lsn the log sequence number to which to set the lsn field
    */
    void SetLSN(lsn_t lsn);

    /**
    * Lookup a bucket page using a directory index
    *
    * @param bucket_idx the index in the directory to lookup
    * @return bucket block_num corresponding to bucket_idx
    */
    int GetBucketBlockNum(uint32_t bucket_idx);

    /**
    * Updates the directory index using a bucket index and block_num
    *
    * @param bucket_idx directory index at which to insert block_num
    * @param bucket_block_num block_num to insert
    */
    void SetBucketBlockNum(uint32_t bucket_idx, int bucket_block_num);


    /**
    * Gets the split image of an index 
    * 
    * @param bucket_idx the directory index for which to find the split image
    * @return the directory index of the split image
    */
    uint32_t GetSplitImageIndex(uint32_t bucket_idx);

    /**
    * GetGlobalDepthMask - returns a mask of global_depth 1's and the rest 0's.
    *
    * In Extendible Hashing we map a key to a directory index
    * using the following hash + mask function.
    *
    * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
    *
    * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
    * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
    * representation.
    *
    * @return mask of global_depth 1's and the rest 0's (with 1's from LSB upwards)
    */
    uint32_t GetGlobalDepthMask();

    /**
    * GetLocalDepthMask - same as global depth mask, except it
    * uses the local depth of the bucket located at bucket_idx
    *
    * @param bucket_idx the index to use for looking up local depth
    * @return mask of local 1's and the rest 0's (with 1's from LSB upwards)
    */
    uint32_t GetLocalDepthMask(uint32_t bucket_idx);


    /**
    * Set the global depth of the hash table directory
    */
    void SetGlobalDepth(uint32_t global_depth);

    /**
    * Get the global depth of the hash table directory
    *
    * @return the global depth of the directory
    */
    uint32_t GetGlobalDepth();

    /**
    * Increment the global depth of the directory
    */
    void IncrGlobalDepth();

    /**
    * Decrement the global depth of the directory
    */
    void DecrGlobalDepth();

    /**
    * @return true if the directory can be shrunk
    * this function will be used to shrink phase in extendible hash
    */
    bool CanShrink();

    /**
    * @return the current directory size
    */
    uint32_t Size();

    /**
    * Gets the local depth of the bucket at bucket_idx
    *
    * @param bucket_idx the bucket index to lookup
    * @return the local depth of the bucket at bucket_idx
    */
    uint32_t GetLocalDepth(uint32_t bucket_idx);

    /**
    * Set the local depth of the bucket at bucket_idx to local_depth
    *
    * @param bucket_idx bucket index to update
    * @param local_depth new local depth
    */
    void SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth);

    
    /**
    * Increment the local depth of the bucket at bucket_idx
    * @param bucket_idx bucket index to increment
    */
    void IncrLocalDepth(uint32_t bucket_idx);

    /**
    * Decrement the local depth of the bucket at bucket_idx
    * @param bucket_idx bucket index to decrement
    */
    void DecrLocalDepth(uint32_t bucket_idx);

    /**
    * Gets the high bit corresponding to the bucket's local depth.
    * This is not the same as the bucket index itself.  This method
    * is helpful for finding the pair, or "split image", of a bucket.
    *
    * @param bucket_idx bucket index to lookup
    * @return the high bit corresponding to the bucket's local depth
    */
    uint32_t GetLocalHighBit(uint32_t bucket_idx);

    /**
    * Prints the current directory
    */
    void PrintDirectory();

private:
    
    // block number
    int block_num_;
    
    // page lsn
    lsn_t lsn_;
    
    uint32_t global_depth_{0};

    uint8_t local_depths_[DIRECTORY_ARRAY_SIZE];
     
    int bucket_block_nums_[DIRECTORY_ARRAY_SIZE];
};

}


#endif