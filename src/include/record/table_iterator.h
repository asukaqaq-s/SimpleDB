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
          tuple_(Tuple()) {}

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
          tuple_(Tuple()) {}

    TableIterator(const TableIterator &other)
        : txn_(other.txn_),
          table_heap_(other.table_heap_),
          rid_(other.rid_),
          tuple_(other.tuple_) {}

    inline void Swap(TableIterator &iter) {
        std::swap(iter.txn_, txn_);
        std::swap(iter.rid_, rid_);
        std::swap(iter.table_heap_, table_heap_);
        std::swap(iter.tuple_, tuple_);
    }


    inline bool operator==(const TableIterator &iter) const {
        return rid_ == iter.rid_ &&
               table_heap_ == iter.table_heap_;
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
    * @brief private method to get the current tuple
    */
    void GetTuple();


private:

    Transaction *txn_;

    TableHeap *table_heap_;

    RID rid_;
    
    Tuple tuple_;
};

} // namespace SimpleDB



#endif








