#ifndef EMBEDDED_RESULTSET_H
#define EMBEDDED_RESULTSET_H

#include "jdbc/resultset_adapter.h"
#include "plan/plan.h"
#include "query/scan.h"
#include "record/schema.h"

namespace SimpleDB {

class EmbeddedConnection;

class EmbeddedResultSet : public ResultSetAdapter {

public:

    EmbeddedResultSet(Plan &pPlan, EmbeddedConnection &pConn);
    
    bool next();
    
    int32_t getInt(const sql::SQLString &pColumnLabel) const;
    
    sql::SQLString getString(const sql::SQLString &pColumnLabel) const;
    
    sql::ResultSetMetaData *getMetaData() const;
    
    void close();

private:

    EmbeddedConnection &connect_;
    
    std::shared_ptr<Scan> scan_;
    
    Schema sch_;


};

} // namespace SimpleDB

#endif