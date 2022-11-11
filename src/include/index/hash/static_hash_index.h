#ifndef STATIC_HASH_INDEX_H
#define STATIC_HASH_INDEX_H

#include "type/value.h"
#include "index/search_key.h"
#include "concurrency/transaction.h"
#include "index/hash/static_hash_table.h"

namespace SimpleDB {
    

class StaticHashIndex {

    static constexpr int BUCKET_NUM = 100;

public:

    StaticHashIndex(Transaction *txn, 
                    std::vector<Value> keys, 
                    StaticHashTable *index) :
            txn_(txn), keys_(keys), index_(index) {}

    


    int GetSearchCost();

private:

    // belong to which transactions
    Transaction *txn_;

    // keys
    std::vector<Value> keys_;

    // indexs
    StaticHashTable *index_;

};


}


#endif