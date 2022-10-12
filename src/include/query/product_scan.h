#ifndef PRODUCT_SCAN_H
#define PRODUCT_SCAN_H

#include "query/scan.h"

namespace SimpleDB {

/**
* @brief the scan class corresponding to the product 
* relational algebra operator.
*/
class ProductScan : public Scan{

public:
    
    /**
    * @brief create a product scan having the two 
    * underlying scans
    * @param s1 the left_child_scan_tree
    * @param s2 the right_child_scan_tree
    */
    ProductScan(const std::shared_ptr<Scan> &scan1,
                const std::shared_ptr<Scan> &scan2);

    /**
    * @brief Position the scan before its first record.
    * In particular, the LHS scan is positioned at its
    * first record, and the RHS scan is positioned
    * BEFORE its first record
    */
    void FirstTuple() override;

    /**
    * @brief Move the scan to the next record.
    * The method moves to the next RHS record, if
    * possible.Otherwise, it moves to the next LHS
    * record and the first RHS record.
    */
    bool Next() override;

    /**
    * @brief returns the integer value of the specified 
    * field.The value is obtained from whichever scan
    * contains the field
    */
    int GetInt(const std::string &field_name) override;

    /**
    * @brief returns the string value of the specified
    * field.
    */
    std::string GetString(const std::string &field_name) override;

    /**
    * @brief returns the value of the specified field
    */
    Constant GetVal(const std::string &field_name) override;

    /**
    * @brief returns true if the specified field is
    * in either of the underlying scans.
    */
    bool HasField(const std::string &field_name) override;

    /**
    * @brief close both underlying scans.
    */
    void Close() override;
    
private:

    std::shared_ptr<Scan> scan1_;
    
    std::shared_ptr<Scan> scan2_;

}; 

} // naemspace SimpleDB


#endif