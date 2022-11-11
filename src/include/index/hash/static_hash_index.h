#ifndef STATIC_HASH_INDEX_H
#define STATIC_HASH_INDEX_H

#include "type/value.h"
#include "index/search_key.h"
#include "concurrency/transaction.h"

namespace SimpleDB {
    

class StaticHashIndex {

    static constexpr int BUCKET_NUM = 100;

public:

    

private:

    // belong to which transactions
    Transaction *txn;

    // 

};


}


#endif