#ifndef EMBEDDED_STATEMENT_CC
#define EMBEDDED_STATEMENT_CC

#include "jdbc/embedded/embedded_statement.h"
#include "jdbc/embedded/embedded_connect.h"
#include "concurrency/transaction.h"

namespace SimpleDB {


EmbeddedStatement::EmbeddedStatement(EmbeddedConnection &connect,
                                     Planner &planner)
    : connect_(connect), planner_(planner) {}

EmbeddedResultSet *EmbeddedStatement::executeQuery(const sql::SQLString &SQL) {
    try {
        Transaction &tx = connect_.getTransaction();
        auto pln = planner_.CreateQueryPlan(SQL.asStdString(), &tx);
        return new EmbeddedResultSet(*pln, connect_);
    } catch (const std::exception &e) {
        connect_.rollback();
        throw sql::SQLException(e.what());
    }
}

int EmbeddedStatement::executeUpdate(const sql::SQLString &SQL) {
    try {
        Transaction &tx = connect_.getTransaction();
        int result = planner_.ExecuteUpdate(SQL.asStdString(), &tx);
        connect_.commit();
        return result;
    } catch (const std::exception &e) {
        connect_.rollback();
        throw sql::SQLException(e.what());
    }
}

void EmbeddedStatement::close() { return; }


} // namespace SimpleDB


#endif