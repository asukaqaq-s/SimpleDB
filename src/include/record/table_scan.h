#ifndef TABLE_SCAN_H
#define TABLE_SCAN_H

#include "record/table_page.h"
#include "record/rid.h"

namespace SimpleDB {
    
class TableScan {

public:

    TableScan(Transaction *txn, std::string table_name, Layout layout);

    /**
    * @brief positioning to the first 
    * tuple of this table
    */
    void FirstTuple();

    /**
    * @brief positioning to the next
    * tuple of this table file
    * will read succeeding blocks in the file
    * until another record is found.
    * @return whether it was successful
    */
    bool NextTuple();

    /**
    * @brief apply to the current record
    * Get a integer value from the specified field
    * @param field_name the specified field
    */
    int GetInt(std::string field_name);

    /**
    * @brief apply to the current record
    * Get a string value from the specified field
    * @param field_name the specified field
    */
    std::string GetString(std::string field_name);

    /**
    * @brief apply to the current record
    * store an integer at the specified field
    * @param field_name the specified field
    * @param val
    */
    void SetInt(std::string field_name, int val);

    /**
    * @brief apply to the current record
    * store an string at the specified field
    * @param field_name the specified field
    * @param val
    */
    void SetString(std::string field_name, std::string val);

    /**
    * @brief 
    */
    bool HasField(std::string field_name);
    
    /**
    * @brief unpinn current block 
    */
    void Close();

    /**
    * @brief find the unused slot in this table file
    * unlike the insertion mehod of record page
    * this methods always succeeds;if it cannot find a place 
    * to insert the record in the existing blocks of the file, 
    * it appends a new block to the file and inserts the record there.
    */
    void NextFreeTuple();

    /**
    * @brief delete current tuple by setting 
    * its flag to empty
    */
    void DeleteTuple();

    /**
    * @brief Move to the specified rid
    */
    void MoveToRid(RID rid);

    /**
    * @brief return the currently rid
    */
    RID GetRid();

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

    std::unique_ptr<TablePage> table_page_;
    
    std::string file_name_;
    
    Layout layout_;
    
    int current_slot_;
};


} // namespace SimpleDB

#endif
