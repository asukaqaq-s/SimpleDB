#ifndef TABLE_PAGE_H
#define TABLE_PAGE_H

#include "concurrency/transaction.h"
#include "buffer/buffer.h"

namespace SimpleDB {


class Transaction;


/**
* @brief 
* 
* sotre a record at a given location in a block
* 
* Page general Header
* -------------
* | PageLsn   |
* -------------
* TablePage Header
* ----------------------------------------------
* | PageLsn | free_space_ptr |  tuple_count_   |
* ----------------------------------------------
* Slot 
* -------------------------------
* | Tuple Start Address Offset  | 
* -------------------------------
* TablePage
* -----------------------------------------------------
* | Header | slot 0 | slot 1 | slot 2 | slot 3 | .... |
* -----------------------------------------------------
* -----------------------------------------------------
* | slot n | free space | tuple 2 | tuple 1 | tuple 0 | 
* -----------------------------------------------------
* Tuple
* ---------------------------
* | Tuple Size | Tuple Data |
* ---------------------------
*
* When the slot array and tuble array meet, the page is full.
* In other words, if the size of free space less than a tuple's 
* size plus a slot's size(sizeof(int)), we can't insert a tuple successfully.
*/
class TablePage : public Buffer {

public:

    TablePage() = default;


    /**
    * @brief we just init tuple_count and free_space_ptr
    * this method shall be called when append a new block in
    * the table file
    */
    void InitPage(Transaction *txn = nullptr, RecoveryManager *rm = nullptr);

    /**
    * @brief read the specified tuple's data
    * 
    * @param RID
    * @param tuple the data which be read
    * @return true if success
    */
    bool GetTuple(const RID &rid, Tuple *tuple);


    /**
    * @brief Without specifying a specific location, 
    * find an empty slot and insert it
    */
    bool Insert(RID *rid, const Tuple &tuple, 
                const std::function<bool(const BlockId &)> &upgrade = nullptr, 
                Transaction *txn = nullptr, RecoveryManager *rm = nullptr);


    /**
    * @brief write data to the specified tuple
    * 
    * @param RID
    * @param tuple
    */
    bool InsertWithRID(const RID &rid, const Tuple &tuple,
                       Transaction *txn = nullptr, RecoveryManager *rm = nullptr);

    /**
    * @brief perform the direct deletion.
    * 
    * @param RID
    * @param tuple the tuple which be deleted
    */
    bool Delete(const RID &rid, Tuple *tuple, 
                Transaction *txn = nullptr, RecoveryManager *rm = nullptr);
    
    /**
    * @brief update a tuple
    * 
    * @param rid
    * @param old_tuple
    * @param new_tuple which be updated
    */
    bool Update(const RID &rid, 
                Tuple *old_tuple, 
                const Tuple &new_tuple,
                Transaction *txn = nullptr, RecoveryManager *rm = nullptr);


    /**
    * @brief get the first exist tuple in the page
    * 
    */
    bool GetFirstTupleRid(RID &next_rid);

    /**
    * @brief get next exist tuple after the specified rid
    * 
    * @param rid modify it to next rid
    */
    bool GetNextTupleRid(RID *rid);
    
    /**
    * @brief get the next empty tuple after the specified rid
    * 
    * @param rid modify it to next rid
    */
    bool GetNextEmptyRid(RID *rid, const Tuple &tuple);


    BlockId GetBlock() const { return block_; }


    Page* GetData() { return data_.get(); }


    std::string ToString(const Schema &schema);
    std::string ToString();

private:

    // static constexpr int DELETE_MASK = 1 << (4 * sizeof(int) - 1);
    static constexpr int PAGE_LSN_OFFSET = 0;
    static constexpr int FREE_SPACE_PTR_OFFSET = PAGE_LSN_OFFSET + sizeof(int);
    static constexpr int TUPLE_COUNT_OFFSET = FREE_SPACE_PTR_OFFSET + sizeof(int);
    static constexpr int SLOT_ARRAY_OFFSET = TUPLE_COUNT_OFFSET + sizeof(int);
    static constexpr int SLOT_SIZE = sizeof(int);
    static constexpr int TUPLE_OFFSET_OFFSET = 0;


    inline void SetTupleCount(int new_count) {
        data_->SetInt(TUPLE_COUNT_OFFSET, new_count);
    }

    inline int GetTupleCount() {
        int res = data_->GetInt(TUPLE_COUNT_OFFSET);
        return res;
    }

    inline void SetFreeSpacePtr(int new_ptr) {
        data_->SetInt(FREE_SPACE_PTR_OFFSET, new_ptr);
    }

    inline int GetFreeSpacePtr() {
        return data_->GetInt(FREE_SPACE_PTR_OFFSET);
    }

    inline int GetTupleSize(int slot) {
        int tuple_offset = GetTupleOffset(slot);
        return data_->GetInt(tuple_offset);
    }

    /**
    * @brief set the record's position
    */
    void SetTupleOffset(int slot, int pos) {
        SIMPLEDB_ASSERT(slot < GetTupleCount(), "overflow");
        int slot_pos = SLOT_ARRAY_OFFSET + SLOT_SIZE * slot;
        data_->SetInt(slot_pos, pos);
    }

    /**
    * @brief get the record's position
    */
    int GetTupleOffset(int slot) {
        SIMPLEDB_ASSERT(slot < GetTupleCount(), "overflow");
        int slot_pos = SLOT_ARRAY_OFFSET + SLOT_SIZE * slot;
        return data_->GetInt(slot_pos);
    }

    /**
    * @brief whether a tuple is deleted
    */
    inline bool IsDeleted(int tuple_offset) {
        return tuple_offset == 0;
    }

    inline int GetFreeSpaceRemaining() {
        return GetFreeSpacePtr() - 
               (SLOT_ARRAY_OFFSET + SLOT_SIZE * GetTupleCount());
    }


};

} // namespace SimpleDB

#endif