#ifndef EMBEDDED_METADATA_H
#define EMBEDDED_METADATA_H

#include "jdbc/resultset_metadata_adapter.h"
#include "record/schema.h"

namespace SimpleDB {

class EmbeddedMetaData : public ResultSetMetaDataAdapter {
public:

    EmbeddedMetaData(const Schema &pSch);

    ~EmbeddedMetaData() = default;

    unsigned int getColumnCount() override;

    sql::SQLString getColumnName(unsigned int pColumn) override;

    int getColumnType(unsigned int pColumn) override;
    
    unsigned int getColumnDisplaySize(unsigned int pColumn) override;

private:
    Schema sch_;
};


} // namespace simpledb


#endif
