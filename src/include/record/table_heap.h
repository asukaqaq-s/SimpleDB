#ifndef TABLE_HEAP_H
#define TABLE_HEAP_H

#include "record/table_page.h"
#include "record/rid.h"
#include "type/value.h"
#include "record/rid.h"
#include "concurrency/lock_manager.h"
#include "record/table_iterator.h"


namespace SimpleDB {
    
class TableHeap {

    friend class TableIterator;

public:

    TableHeap(Transaction *txn, std::string table_name, FileManager *file_mgr,
              RecoveryManager *rm, BufferManager *bfm, LockManager *lock);

    ~TableHeap() {}


    /**
    * @brief positioning to the next tuple of this table file
    * will read succeeding blocks in the file until another 
    * record is found.
    * @param tuple data
    * @return whether it was successful
    */
    bool Next();

    /**
    * @brief insert a tuple without sepcifie which rid and modify rid.
    * 
    * @param tuple 
    * @param rid 
    */
    void Insert(Transaction *txn, const Tuple &tuple, RID *rid);
    

    bool GetTuple(Transaction *txn, const RID &rid, Tuple *tuple);

    /**
    * @brief find the unused slot in this table file
    * unlike the insertion mehod of record page
    * this methods always succeeds;if it cannot find a place 
    * to insert the record in the existing blocks of the file, 
    * it appends a new block to the file and inserts the record there.
    */
    bool InsertWithRid(Transaction *txn, const Tuple &tuple, const RID &rid);

    /**
    * @brief delete current tuple by setting 
    * its flag to empty
    */
    void Delete(Transaction *txn, const RID& rid);

    /**
    * @brief update the specified tuple
    * if we can not update it in this tuple, we should abort this txn
    */
    bool Update(Transaction *txn, const RID& rid, const Tuple &new_tuple);
    
    
    void CreateNewBlock(Transaction *txn);

    int GetFileSize(Transaction *txn);

    BlockId Append(Transaction *txn);

    
    TableIterator Begin(Transaction *txn);

    TableIterator End();

private:

    void MoveToBlock(RID &rid, int block_number);
    
    void MoveToNewBlock(Transaction *txn, RID &rid);

    bool AtLastBlock(Transaction *txn, int block_number);

    
    BlockId GetBlock(RID rid) {
        return BlockId(file_name_, rid.GetBlockNum());
    }
    

private:
    
    std::string file_name_;

    FileManager *file_manager_;

    RecoveryManager *recovery_manager_;

    BufferManager *buffer_pool_manager_;

    LockManager *lock_manager_;
};


} // namespace SimpleDB

#endif
