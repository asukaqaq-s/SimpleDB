#ifndef TABLE_ITERATOR_H
#define TABLE_ITERATOR_H

#include "record/table_heap.h"


namespace SimpleDB {

class TableHeap;

/**
* @brief 
* iterator of table. every tableiterators only server for one transaction.
* so we need acquire s-lock before read a block
*/
class TableIterator {
public:

    /**
    * @brief
    * Initialize an invalid table iterator
    */
    TableIterator()
        : txn_(nullptr),
          table_heap_(nullptr),
          rid_(RID()),
          tuple_(Tuple()) { }

    /**
    * @brief
    * Initialize table iterator based on table heap
    * @param table_heap 
    * @param rid 
    */
    TableIterator(Transaction *txn, TableHeap *table_heap, RID rid)
        : txn_(txn),
          table_heap_(table_heap),
          rid_(rid),
          tuple_(Tuple()) {

            // cache a table page
            if (rid_.GetBlockNum() != -1) {
                table_page_ = static_cast<TablePage*> (txn_->PinBlock(GetBlock()));
            }
          }

    TableIterator(const TableIterator &other)
        : txn_(other.txn_),
          table_heap_(other.table_heap_),
          rid_(other.rid_),
          tuple_(other.tuple_) {
            
            // we should pin this table page again
            if (rid_.GetBlockNum() != -1) {
                table_page_ = static_cast<TablePage*> (txn_->PinBlock(GetBlock()));
            }
          }

    ~TableIterator() {
        Close();
    }

    inline void Swap(TableIterator &iter) {
        std::swap(iter.txn_, txn_);
        std::swap(iter.rid_, rid_);
        std::swap(iter.table_heap_, table_heap_);
        std::swap(iter.tuple_, tuple_);
        std::swap(iter.table_page_, table_page_);  
    }


    inline bool operator==(const TableIterator &iter) const {
        return rid_ == iter.rid_ &&
               table_heap_ == iter.table_heap_ &&
               table_page_ == iter.table_page_;
    }

    inline bool operator!=(const TableIterator &iter) const {
        return !((*this) == iter);
    }

    
    TableIterator& operator=(const TableIterator &iter);

    const Tuple &operator*();

    Tuple *operator->();

    TableIterator operator++();

    TableIterator operator++(int);

    RID GetRID();

    BlockId GetBlock();

    // provide interfaces like index iterator

    /**
     * @brief 
     * Get tuple. behaviour is the same as operator*
     * @return Tuple 
     */
    const Tuple &Get();

    /**
     * @brief 
     * Check whether current iterator reaches the end. i.e. this == End()
     * @return true 
     * @return false 
     */
    bool IsEnd();


private:
    
    /**
    * @brief use close to replace unpin
    */
    void Close();
    

    /**
    * @brief private method to get the current tuple
    */
    void GetTuple();


private:

    // txn provides us with execution context
    Transaction *txn_;

    // include some informations of table
    TableHeap *table_heap_;

    // current position
    RID rid_;

    // cache table page to reduce system calls
    TablePage* table_page_{nullptr};

    // cache tuple to reduce copy
    Tuple tuple_;
};

} // namespace SimpleDB



#endif








