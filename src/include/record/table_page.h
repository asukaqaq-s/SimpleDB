#ifndef TABLE_PAGE_H
#define TABLE_PAGE_H

#include "concurrency/transaction.h"
#include "record/layout.h"

namespace SimpleDB {

enum TupleStatus {
    EMPTY = 0,
    USED,
}; 
/**
* @brief sotre a record at a given location in a block
* 
* TablePage Header
* ------------------------------------
* | free_space_ptr |  tuple_count_   |
* ------------------------------------
* Slot 
* --------------------------------------------
* | Tuple Start Address Offset  |
* --------------------------------------------
* TablePage
* -----------------------------------------------------
* | Header | slot 0 | slot 1 | slot 2 | slot 3 | .... |
* -----------------------------------------------------
* -----------------------------------------------------
* | slot n | free space | tuple 2 | tuple 1 | tuple 0 | 
* -----------------------------------------------------
* When the slot array and tuble array meet, the page is full.
* In other words, the size of free space less than a tuple's size plus a slot's size(sizeof(int))
*/
class TablePage {

public:

    TablePage() = default;
    /**
    * @brief 
    * 
    * @param txn created by which txn
    * @param block the corresponding block
    * @param layout the format of a record
    */
    TablePage(Transaction *txn, BlockId block, Layout layout);

    /**
    * @brief we just init tuple_count and free_space_ptr
    * 
    */
    void InitPage();

    /**
    * @brief return the integer value stored for
    * the specified field of a specified slot.
    * @param slot 
    * @param field_name the name of the field in slot
    * @return the integer stored in that field 
    */
    int GetInt(int slot, std::string field_name);

    /**
    * @brief return the string value stored for
    * the specified field of a specified slot.
    * @param slot 
    * @param field_name the name of the field in slot
    * @return the string stored in that field 
    */
    std::string GetString(int slot, std::string field_name);

    /**
    * @brief store an integer at the specified field
    * of the specified slot
    * @param field_name the name of the field
    * @param val the integer value stored in that field
    */
    void SetInt(int slot, std::string field_name, int val);

    /**
    * @brief store an string at the specified field
    * of the specified slot
    * @param field_name the name of the field
    * @param val the string value stored in that field
    */
    void SetString(int slot, std::string field_name, std::string val);

    /**
    * @brief Complete the delete a tuple
    * by setting the usage flag of a tuple to EMPTY.
    */
    void MarkDeleteTuple(int slot);

    /** 
    * @brief find the first used tuple after the specified slot
    * 
    * @param slot the specified slot
    */
    int NextTuple(int slot);

    /**
    * @brief find the first unused tuple after the specified slot
    * 
    * @param slot the specified slot
    */
    int NextEmptyTuple(int slot);

    /**
    * @return the record's blockid
    */
    BlockId GetBlock() { return block_; }

    bool IsValidTuple(int slot);


private:

    static constexpr int DELETE_MASK = 1 << (4 * sizeof(int) - 1);
    static constexpr int FREE_SPACE_PTR_OFFSET = 0;
    static constexpr int TUPLE_COUNT_OFFSET = FREE_SPACE_PTR_OFFSET + sizeof(int);
    static constexpr int SLOT_ARRAY_OFFSET = TUPLE_COUNT_OFFSET + sizeof(int);
    static constexpr int SLOT_SIZE = sizeof(int);
    static constexpr int TUPLE_OFFSET_OFFSET = 0;


    inline void SetTupleCount(int new_count) {
        txn_->SetInt(block_, TUPLE_COUNT_OFFSET,new_count, true);
    }

    inline int GetTupleCount() {
        return txn_->GetInt(block_, TUPLE_COUNT_OFFSET);
    }

    inline void SetFreeSpacePtr(int new_ptr) {
        txn_->SetInt(block_, FREE_SPACE_PTR_OFFSET, new_ptr, true);
    }

    inline int GetFreeSpacePtr() {
        return txn_->GetInt(block_, FREE_SPACE_PTR_OFFSET);
    }

    inline int GetTupleSize(int slot) {
        int tuple_offset = GetTupleOffset(slot);
        return txn_->GetInt(block_, tuple_offset);
    }

    /**
    * @brief set the record's position
    */
    void SetTupleOffset(int slot, int pos);

    /**
    * @brief get the record's position
    */
    int GetTupleOffset(int slot);

    /**
    * @brief we just delete a tuple by setting the highest bit
    * of tuple offset
    */
    int SetDeleteFlag(int tuple_offset) {
        return tuple_offset | DELETE_MASK;
    }

    /**
    * @brief realloc a tuple by clearing the highest bit
    * of tuple offset
    */
    int UnsetDeleteFlag(int tuple_offset) {
        return tuple_offset & (~DELETE_MASK);
    }

    /**
    * @brief whether a tuple is deleted
    */
    bool IsDeleted(int tuple_offset) {
        return static_cast<bool>(tuple_offset & DELETE_MASK) ||
               tuple_offset == 0;
    }

    /**
    * @brief Does this page have enough space 
    * to insert a new tuple
    * 
    * @param tuple_size the size of the tuple that will be inserted
    * @return true if can insert the new one
    */
    bool CanInsert(int tuple_size);

    /**
    * @brief search a tuple which satisfy flag 
    * after the specified slot
    * @return The slot number of the tuple found
    */
    int SearchTuple(int slot, TupleStatus flag);

private:

    // be created by which transacations
    Transaction *txn_;
    
    BlockId block_;
    // indicates the format of a record
    Layout layout_;
};

} // namespace SimpleDB

#endif