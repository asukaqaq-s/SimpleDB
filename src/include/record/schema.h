#ifndef SCHEMA_H
#define SCHEMA_H

#include "type/typeid.h"
#include "config/macro.h"
#include "record/column.h"

#include <vector>
#include <string>
#include <algorithm>
#include <map>
#include <iostream>

namespace SimpleDB {

/**
 * @brief 
 * table schema, contains the column information of this table.
 * in schema we also caculate the total length of columns. it is
 * useful in tuple class.
 */
class Schema {


public:

    Schema() = default;

    Schema(const std::vector<Column> &columns) : columns_(columns) {
        CaculateSchema();
    }
    
    Schema(const Schema& schema) {
        AddAllColumns(schema);
    }

    
    Schema(int length, const std::vector<Column> &columns) :
        length_(length), columns_(columns) {}


    void AddSameColumn(const std::string &column_name, const Schema &other) {
        AddColumn(other.GetColumn(column_name));
    }


    void AddColumn(Column column) {
        column.column_offset_ = length_;
        length_ += column.GetFixedLength();
        columns_.push_back(column);
    }

    void AddAllColumns(const Schema& schema) {
        for (auto t:schema.columns_) {
            columns_.push_back(t);
        }
        CaculateSchema();
    }

    int GetLength() const {
        return length_;
    }

    inline bool HasColumn(const std::string &column_name) const {
        int size = columns_.size();
        for (int i = 0;i < size;i ++) {
            if (columns_[i].column_name_ == column_name) {
                return true;
            }
        }
        return false;
    }
    
    inline int GetColumnIdx(const std::string &column_name) const {

        int size = columns_.size();
        for (int i = 0;i < size;i ++) {
            if (columns_[i].column_name_ == column_name) {
                return i;
            }
        }
        
        return -1;
    }

    inline const Column &GetColumn(int index) const {
        return columns_[index];
    }

    inline const Column &GetColumn(const std::string &column_name) const {
        int pos = GetColumnIdx(column_name);
        SIMPLEDB_ASSERT(pos != -1, "the column_name is not exist");
        return columns_[pos];
    }

    inline const std::vector<Column> &GetColumns() const {
        return columns_;
    }
    
    int GetColumnsCount() const {
        return columns_.size();
    }

    bool operator == (const Schema &schema) const {
        return length_ == schema.length_ &&
               columns_ == schema.columns_;
    }

    Schema& operator =(const Schema &other) {
        if (&other != this) {
            length_ = other.length_;
            columns_ = other.columns_;
        }
        return *this;
    }


    std::string ToString() {
        std::stringstream str;
        int size = columns_.size();

        for (int i = 0;i < size; i++) {
            str << "column " << i << " "
                << columns_[i].ToString();
        }
        return str.str();
    }
    

private:

    void CaculateSchema() {

        int cur_length = 0;
        for (auto &t:columns_) {
            t.column_offset_ = cur_length;
            cur_length += t.GetFixedLength();
        }

        length_ = cur_length;
    }

    int length_{0};

    // the list of fields/columns
    std::vector<Column> columns_;


};

} // namespace SimpleDB

#endif