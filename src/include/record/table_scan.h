#ifndef TABLE_SCAN_H
#define TABLE_SCAN_H

#include "record/table_page.h"
#include "record/rid.h"
#include "query/update_scan.h"

namespace SimpleDB {
    
class TableScan : public UpdateScan{

public:

    TableScan(Transaction *txn, std::string table_name, Layout layout);

    ~TableScan() override{}

    /**
    * @brief positioning to the first 
    * tuple of this table
    */
    void FirstTuple() override;

    /**
    * @brief positioning to the next
    * tuple of this table file
    * will read succeeding blocks in the file
    * until another record is found.
    * @return whether it was successful
    */
    bool Next() override;

    /**
    * @brief apply to the current record
    * Get a integer value from the specified field
    * @param field_name the specified field
    */
    int GetInt(const std::string &field_name) override;

    /**
    * @brief apply to the current record
    * Get a string value from the specified field
    * @param field_name the specified field
    */
    std::string GetString(const std::string &field_name) override;

    /**
    * @brief apply to the current record
    * Get a constant object from the spefied field
    * @param field_name the specified field
    */
    Constant GetVal(const std::string &field_name) override;

    /**
    * @brief apply to the current record
    * store an integer at the specified field
    * @param field_name the specified field
    * @param val
    */
    void SetInt(const std::string &field_name, int val) override;
 
    /**
    * @brief apply to the current record
    * store an string at the specified field
    * @param field_name the specified field
    * @param val
    */
    void SetString(const std::string &field_name, 
                   const std::string &val) override;

    /**
    * @brief apply to the current record
    * store an string at the specified field
    * @param field_name the specified field
    * @param val
    */
    void SetVal(const std::string &field_name, 
                const Constant &val) override;

    /**
    * @brief 
    */
    bool HasField(const std::string &field_name) override;
    
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
    void Insert() override;

    /**
    * @brief delete current tuple by setting 
    * its flag to empty
    */
    void Remove() override;

    /**
    * @brief Move to the specified rid
    */
    void MoveToRid(const RID &rid) override;

    /**
    * @brief return the currently rid
    */
    RID GetRid() override;

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
