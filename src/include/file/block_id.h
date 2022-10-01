#ifndef BLOCK_ID_H
#define BLOCK_ID_H

#include <string>

namespace SimpleDB {

/**
* @brief we use page-level to access a file that identified by a file name.
* a file consists of many disk-blocks that identified by a logical block number.
* so, blockid consist of a filename and a logical block number
* A disk-block can be uniquely identified by BlockId 
*/
class BlockId {
    
    // friendly function
    friend bool operator == (const BlockId &lobj, const BlockId &robj);
    friend bool operator != (const BlockId &lobj, const BlockId &robj);
    friend bool operator < (const BlockId &lobj, const BlockId &robj);
    friend bool operator > (const BlockId &lobj, const BlockId &robj);
    friend bool operator <= (const BlockId &lobj, const BlockId &robj);
    friend bool operator >= (const BlockId &lobj, const BlockId &robj);

public:

    BlockId& operator= (const BlockId &obj);
    /**
    * @brief non-parameters constructor 
    */
    BlockId() {}
    
    /**
    * @brief copy-constructor
    */
    BlockId(const BlockId &b) : 
        file_name_(b.FileName()), block_num_(b.BlockNum()) {} 
    /**
    * @brief Construct a Blockid object
    * 
    * @param file_name the file corresponding to the block
    * @param block_num the block's logical number in file
    */
    BlockId(std::string file_name, int block_num) : 
        file_name_(file_name), block_num_(block_num) {}
    
    /**
    * @brief
    *  
    * @return file_name_
    */
    std::string FileName() const;

    /**
    * @brief
    *  
    * @return block_num_
    */
    int BlockNum() const;
    
    /**
    * @brief whether obj equal to this one?
    * 
    * @param
    * @return if equal,return true
    *         else return false
    */
    bool equals(const BlockId &obj) const;

    /**
    * @brief for debugging purpose
    * 
    * @return the information of the block
    */
    std::string to_string() const;
    
    

private:
    // the block belong to which file
    std::string file_name_;
    // logical block number
    int block_num_;
};
}



#endif
