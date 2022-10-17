#ifndef DRIVER_ADAPTER_H
#define DRIVER_ADAPTER_H

#include "mysql-connector-c++/cppconn/driver.h"

namespace SimpleDB {
class DriverAdapter : public sql::Driver {
public:
  
  virtual sql::Connection *connect(const sql::SQLString &hostName,
                                   const sql::SQLString &userName,
                                   const sql::SQLString &password) override;
  
  virtual sql::Connection *connect(sql::ConnectOptionsMap &options) override;
  
  virtual int getMajorVersion() override;
  
  virtual int getMinorVersion() override;
  
  virtual int getPatchVersion() override;
  
  virtual const sql::SQLString &getName() override;
  
  virtual void threadInit() override;
  
  virtual void threadEnd() override;
};
} // namespace simpledb


#endif