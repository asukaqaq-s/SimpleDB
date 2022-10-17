#ifndef EMBEDDED_CONNECT_CC
#define EMBEDDED_CONNECT_CC

#include "jdbc/embedded/embedded_connect.h"

namespace SimpleDB {

EmbeddedConnection::EmbeddedConnection(std::unique_ptr<SimpleDB> db)
    : db_(std::move(db)), current_tx_(db_->GetNewTxn()), 
      planner_(db_->GetPlanner()) {}

EmbeddedStatement *EmbeddedConnection::createStatement() {
    return new EmbeddedStatement(*this, planner_);
}

void EmbeddedConnection::close() { 
    current_tx_->Commit(); 
}

void EmbeddedConnection::commit() {
    current_tx_->Commit();
    current_tx_ = db_->GetNewTxn();
}

void EmbeddedConnection::rollback() {
    current_tx_->RollBack();
    current_tx_ = db_->GetNewTxn();
}

Transaction &EmbeddedConnection::getTransaction() { 
    return *current_tx_; 
}


} // namespace SimpleDB


#endif