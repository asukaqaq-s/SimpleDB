#ifndef EMBEDDED_DRIVER_H
#define EMBEDDED_DRIVER_H

#include "jdbc/driver_adapter.h"
#include "jdbc/embedded/embedded_connect.h"

namespace SimpleDB {
class EmbeddedDriver : public DriverAdapter {
public:
    
    EmbeddedConnection *connect(const sql::SQLString &hostName,
                                const sql::SQLString &userName,
                                const sql::SQLString &password);
};
} // namespace simpledb

#endif