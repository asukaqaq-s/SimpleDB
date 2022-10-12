#ifndef PROJECT_SCAN_H
#define PROJECT_SCAN_H

#include "query/scan.h"

#include <vector>

namespace SimpleDB {

/**
* @brief the scan class corresponding to the PROJECT relational 
* algebra operator.
* all methods except hasfield delegate their work to the 
* underlying scan.
*/
class ProjectScan : public Scan {

public:

    /**
    * @brief Create a project scan having the specified underlying 
    * scan and field list.
    * @param s the underlying scan
    * @param field_lsit the list of field names
    */
    ProjectScan(const std::shared_ptr<Scan> &scan, 
                const std::vector<std::string> &field_list);

    void FirstTuple() override;

    bool Next() override;

    int GetInt(const std::string &field_name) override;

    std::string GetString(const std::string &field_name) override;

    Constant GetVal(const std::string &field_name) override;

    bool HasField(const std::string &field_name) override;

    void Close() override;

private:

    std::shared_ptr<Scan> scan_;

    std::vector<std::string> field_list_;

};

} // namespace SimpleDB

#endif