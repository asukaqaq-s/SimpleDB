#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <string>

namespace SimpleDB {

/**
* @brief a transaction object 
* which contains the context required for transaction execution
*/
class Transaction {

public:


    void Pin(BlockId block) {}
    
    void Unpin(BlockId block) {}

    bool SetString(BlockId block, int offset, std::string new_value, bool is_log) {}
    
    std::string GetString(BlockId block, int offset) {}
    
    bool SetInt(BlockId block, int offset, int new_value,bool is_log) {}

    int GetInt(BlockId block, int offset) {}

};

} // namespace SimpleDB

#endif
