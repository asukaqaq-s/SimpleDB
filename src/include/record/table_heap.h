#ifndef TABLE_HEAP_H
#define TABLE_HEAP_H

#include "record/table_page.h"
#include "record/rid.h"
#include "type/value.h"
#include "record/rid.h"
#include "concurrency/lock_manager.h"
#include "record/table_iterator.h"


namespace SimpleDB {
    
class TableIterator;

class TableHeap {

    friend class TableIterator;

public:

    TableHeap(Transaction *txn, std::string table_name, FileManager* file_manager_,
              RecoveryManager *recover_manager, BufferManager *buffer_pool_manager_);

    ~TableHeap() {}

    /**
    * @brief insert a tuple without specifie which rid and modify this rid.
    * this methods always succeeds;if it cannot find a place to insert the 
    * record in the existing blocks of the file, it appends a new block to 
    * the file and inserts the record there.
    * @param tuple 
    * @param rid 
    */
    void Insert(Transaction *txn, const Tuple &tuple, RID *rid);
    

    /**
    * @brief return the specified tuple
    */
    bool GetTuple(Transaction *txn, const RID &rid, Tuple *tuple);


    /**
    * @brief insert a tuple in the specified position
    */
    bool InsertWithRid(Transaction *txn, const Tuple &tuple, const RID &rid);


    /**
    * @brief apply delete the specified tuple
    */
    void Delete(Transaction *txn, const RID& rid);


    /**
    * @brief update the specified tuple
    * if we can not update it in this tuple, should abort this txn
    */
    bool Update(Transaction *txn, const RID& rid, const Tuple &new_tuple);
    
    /**
    * @brief return the size of table file
    */
    int GetFileSize(Transaction *txn);

    BlockId AppendBlock(Transaction *txn);

    
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

};


} // namespace SimpleDB

#endif
