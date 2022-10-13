#ifndef CREATE_VIEW_DATA_H
#define CREATE_VIEW_DATA_H

#include "parse/object.h"
#include "parse/query_data.h"

#include <memory>
#include <set>

namespace SimpleDB {
    
class CreateViewData {

public:
    
    CreateViewData(const std::string &view_name,
                   std::unique_ptr<QueryData> query_data)
        : view_name_(view_name), query_data_(std::move(query_data)) {}

    
    /**
    * @brief return the name of the new view
    */
    std::string GetViewName() const {
        return view_name_;
    }

    /**
    * @brief return the definiton of the new view.
    */
    std::string GetViewDef() const {
        return query_data_->ToString();
    }

private:

    std::string view_name_;
    
    std::unique_ptr<QueryData> query_data_;

};

} // namespace SimpleDB

#endif
