#ifndef TABLE_SCAN_H
#define TABLE_SCAN_H

#include "record/table_page.h"
#include "record/rid.h"
#include "type/constant.h"
#include "record/rid.h"
// #include "query/update_scan.h"

namespace SimpleDB {
    
class TableScan {

public:

    TableScan(Transaction *txn, std::string table_name, Layout layout);

    ~TableScan() {}

    /**
    * @brief positioning to the first 
    * tuple of this table
    */
    void FirstTuple();

    /**
    * @brief positioning to the next tuple of this table file
    * will read succeeding blocks in the file until another 
    * record is found.
    * @param tuple data
    * @return whether it was successful
    */
    bool Next();

    void NextInsert(const Tuple &tuple);
    

    bool GetTuple(Tuple *tuple);

    /**
    * @brief 
    */
    bool HasField(const std::string &field_name);

    /**
    * @brief find the unused slot in this table file
    * unlike the insertion mehod of record page
    * this methods always succeeds;if it cannot find a place 
    * to insert the record in the existing blocks of the file, 
    * it appends a new block to the file and inserts the record there.
    */
    bool Insert(const Tuple &tuple);

    /**
    * @brief delete current tuple by setting 
    * its flag to empty
    */
    void Delete();

    /**
    * @brief update the specified tuple
    * if we can not update it in this tuple, we should abort this txn
    */
    bool Update(const Tuple &new_tuple);

    /**
    * @brief Move to the specified rid
    */
    void MoveToRid(const RID &rid);

    /**
    * @brief return the currently rid
    */
    inline RID GetRid() {
        return rid_;
    }

    inline BlockId GetBlock() {
        return BlockId(file_name_, rid_.GetBlockNum());
    }

private:
    
    /**
    * @brief Move to the first tuple to 
    * the specified block
    */
    void MoveToBlock(int block_number);

    /**
    * @brief create a new block and move to it
    */
    void MoveToNewBlock();

    /**
    * @brief whether the last block in the table
    */
    bool AtLastBlock();
    
private:
    
    Transaction *txn_;
    
    std::string file_name_;
    
    Layout layout_;

    RID rid_;
};


} // namespace SimpleDB

#endif
