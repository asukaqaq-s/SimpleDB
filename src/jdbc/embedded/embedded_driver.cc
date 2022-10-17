#ifndef EMBEDDED_DRIVER_CC
#define EMBEDDED_DRIVAER_CC

#include "jdbc/embedded/embedded_driver.h"

namespace SimpleDB {

EmbeddedConnection *EmbeddedDriver::connect(const sql::SQLString &host_name,
                                            const sql::SQLString &user_name,
                                            const sql::SQLString &password) {
    sql::SQLString url = user_name + ":" + password + "@" + host_name;
    auto db = std::make_unique<SimpleDB>(url.asStdString());
    return new EmbeddedConnection(std::move(db));
}


} // namespace simpledb

#endif
