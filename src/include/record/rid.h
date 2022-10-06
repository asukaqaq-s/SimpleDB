#ifndef RID_H
#define RID_H

#include <string>

namespace SimpleDB {

/**
* @brief An identifier for a record within a file.
* A RID consists of the block number in the file,
* and the location of the record in that block.
* 
*/
class RID {

public:

    RID(int block_number, int slot) :
        block_number_(block_number), slot_(slot) {}
    
    int GetBlockNum() { return block_number_; }

    int GetSlot() { return slot_; }

    bool operator == (const RID& obj) const {
        return block_number_ == obj.block_number_ &&
               slot_ == obj.slot_;
    }

    std::string ToString() {
        return "[ block_number = " + std::to_string(block_number_)
            + ", slot = " + std::to_string(slot_) + "]";
    }

private:
    
    // logical block number in this table file
    int block_number_;
    // slot number
    int slot_; 
}; 

} // namespace SimpleDB

#endif