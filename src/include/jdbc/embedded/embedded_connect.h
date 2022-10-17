#ifndef EMBEDDED_CONNECT_H
#define EMBEDDED_CONNECT_H

#include "jdbc/connection_adapter.h"
#include "jdbc/embedded/embedded_statement.h"
#include "server/simpledb.h"
#include "concurrency/transaction.h"

namespace SimpleDB {

class EmbeddedConnection : public ConnectionAdapter {

public:

    EmbeddedConnection(std::unique_ptr<SimpleDB> pDb);

    EmbeddedStatement *createStatement() override;
    
    void close() override;
    
    void commit() override;
    
    void rollback() override;
    
    Transaction &getTransaction();

private:

    std::unique_ptr<SimpleDB> db_;

    std::unique_ptr<Transaction> current_tx_;

    Planner &planner_;

};

} // namespace SimpleDB

#endif