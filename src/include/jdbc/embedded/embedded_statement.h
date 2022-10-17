#ifndef EMBEDDED_STATEMENT_H
#define EMBEDDED_STATEMENT_H

#include "jdbc/embedded/embedded_resultset.h"
#include "jdbc/statement_adapter.h"
#include "plan/planner.h"

namespace SimpleDB {

class EmbeddedConnection;

class EmbeddedStatement : public StatementAdapter {
public:

    EmbeddedStatement(EmbeddedConnection &pConn, Planner &pPlanner);
    
    EmbeddedResultSet *executeQuery(const sql::SQLString &pSQL);

    int executeUpdate(const sql::SQLString &pSQL);

    void close();

private:

    EmbeddedConnection &connect_;
    Planner &planner_;
};

} // namespace SimpleDB

#endif