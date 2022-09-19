#ifndef LOGMANAGER_H
#define LOGMANAGER_H

#include "../file/FileManager.h"
#include "../file/FileManager.cc"

namespace SimpleDB {

/**
* @brief 
    The DB has a log file and only one LogManager Object in system.
The LogManager object is used to write and read a log-record in log file,
and the object is called by the recovey system to restore data
which stored in disk through the log.
    Note that, LogManager does not know the meaning of the stored data, it is
only responsible for storage and not responsible for recovery data.
*/

class LogManager {

public:

private:
        
};

} // namespace SimpleDB
#endif