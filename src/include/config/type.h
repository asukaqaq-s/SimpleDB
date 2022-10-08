#ifndef TYPE_H
#define TYPE_H

namespace SimpleDB {

using frame_id_t = int; /* bufferpool */ 
using lsn_t = int;    /* log */
using txn_id_t = int; /* transaction */


static constexpr int INVALID_FRAME_ID = -1;
static constexpr int INVALID_TXN_ID = -1;
static constexpr int INVALID_LSN = -1;

// LENGTH LIMIT
static constexpr int MAX_TABLE_NAME_LENGTH = 16;
static constexpr int MAX_VIEW_LENGTH = 100;

// THRESHOLD LIMIT
static constexpr int MAX_REFERSH_THRESHOLDS = 100;


}// namespace SimpleDB

#endif