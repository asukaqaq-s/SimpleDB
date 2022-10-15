#ifndef PLAN_H
#define PLAN_H

#include "query/scan.h"
#include "record/schema.h"


namespace SimpleDB {

/**
* @brief the interface implemented by each query plan
* there is a plan class for each relational algebra operator
* the difference between plan and scan is that
* a plan accesses the metadata of the tables in the query, 
* a scan accesses their data.
*/
class Plan {

public:

    virtual ~Plan() = default;

    /**
    * @brief opens a scan corresponding to this plan.
    * the scan will be positioned before its first record.
    * @return a scan
    */
    virtual std::shared_ptr<Scan> Open() = 0;

    /**
    * @brief returns an estimate of the number of block
    * accesses that will occur then the scan is read to comletion.
    * @return the estimated number 
    */
    virtual int GetAccessBlocks() = 0;

    /**
    * @brief Returns an estimate of the number of records
    * in the query's output table.
    * @return the estimated number of output records
    */
    virtual int GetOutputTuples() = 0;
   
   /**
    * @brief an estimate of the number of distinct values
    * for the specified field in the query's output table.
    * @param fldname the name of a field
    * @return the estimated number of distinct field values in the output
    */
    virtual int GetDistinctVals(const std::string &fldname) = 0;

    /**
    * @brief returns the schema of the query.
    * @return the query's schema
    */
    virtual Schema GetSchema() = 0;

};

} // namespace SimpleDB

#endif
