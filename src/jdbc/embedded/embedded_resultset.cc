#ifndef EMBEDDED_RESULTSET_CC
#define EMBEDDED_RESULTSET_CC

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>

#include "jdbc/embedded/embedded_connect.h"
#include "jdbc/embedded/embedded_metadata.h"
#include "jdbc/embedded/embedded_resultset.h"

namespace SimpleDB {
    
EmbeddedResultSet::EmbeddedResultSet(Plan &pPlan, EmbeddedConnection &pConn)
    : connect_(pConn), scan_(pPlan.Open()), sch_(pPlan.GetSchema()) {}

bool EmbeddedResultSet::next() {
    try {
        return scan_->Next();
    }
    catch (const std::exception &e) {
        connect_.rollback();
        throw sql::SQLException(e.what());
    }
}

int32_t EmbeddedResultSet::getInt(const sql::SQLString &column_name) const {
    try {
        std::string field_name = column_name.asStdString();
        // all fieldname store in SimpleDB is lower case
        std::transform(field_name.begin(), field_name.end(), field_name.begin(),
                    [](unsigned char c) { return std::tolower(c); });
        return scan_->GetInt(field_name);
    } 
    catch (const std::exception &e) {
        connect_.rollback();
        throw sql::SQLException(e.what());
    }
}

sql::SQLString
EmbeddedResultSet::getString(const sql::SQLString &column_name) const {
    try {
        std::string field_name = column_name.asStdString();
        std::transform(field_name.begin(), field_name.end(), field_name.begin(),
                    [](unsigned char c) { return std::tolower(c); });
        return scan_->GetString(field_name);
    } 
    catch (const std::exception &e) {
        connect_.rollback();
        throw sql::SQLException(e.what());
    }
}

sql::ResultSetMetaData *EmbeddedResultSet::getMetaData() const {
    return new EmbeddedMetaData(sch_);
}

void EmbeddedResultSet::close() {
    scan_->Close();
    connect_.commit();
}


} // namespace SimpleDB


#endif