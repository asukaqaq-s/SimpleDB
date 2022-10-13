#ifndef QUERY_DATA_H
#define QUERY_DATA_H

#include "query/predicate.h"

#include <vector>
#include <set>

namespace SimpleDB {

class QueryData {
    
public:

    QueryData(const std::vector<std::string> &fields, 
              const std::set<std::string> &tables,
              const Predicate &pred) :
              fields_(fields_), tables_(tables), pred_(pred) {}



    std::vector<std::string> GetFields() const {
        return fields_;
    }

    std::set<std::string> GetTables() const {
        return tables_;
    }

    Predicate GetPred() const {
        return pred_;
    }

    std::string ToString() {
        std::string res = "select ";
        
        for (const auto &s: fields_) {
            res += s + ", ";
        }
        
        // remove final comma and space :", "
        res = res.substr(0, res.size() - 2);
        res += " from ";

        for (const auto &s: tables_) {
            res += ", ";
        }
        
        // remove final comma and space :", "
        res = res.substr(0, res.size() - 2);
        std::string pred_str = pred_.ToString();
        if (!pred_str.empty()) {
            res += " where " + pred_str;
        }

        return res;
    }

private:
    
    std::vector<std::string> fields_;

    std::set<std::string> tables_;
    
    Predicate pred_;
};
    

} // namespace SimpleDB

#endif
