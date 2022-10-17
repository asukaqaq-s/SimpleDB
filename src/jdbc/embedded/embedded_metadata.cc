#ifndef EMBEDDED_METADATA_CC
#define EMBEDDED_METADATA_CC

#include <algorithm>

#include "jdbc/embedded/embedded_metadata.h"

namespace SimpleDB {
EmbeddedMetaData::EmbeddedMetaData(const Schema &sch) : sch_(sch) {}

unsigned int EmbeddedMetaData::getColumnCount() { 
    return sch_.GetFields().size(); 
}

sql::SQLString EmbeddedMetaData::getColumnName(unsigned int column) {
    return sql::SQLString(sch_.GetFields()[column - 1]);
}

int EmbeddedMetaData::getColumnType(unsigned int column) {
    std::string field_name = getColumnName(column).asStdString();
    return sch_.GetType(field_name);
}

unsigned int EmbeddedMetaData::getColumnDisplaySize(unsigned int column) {
    std::string field_name = getColumnName(column).asStdString();
    int field_type = sch_.GetType(field_name);
    int field_length = (field_type == FieldType::INTEGER) ? 6 : sch_.GetLength(field_name);

    return std::max(static_cast<int>(field_name.size()), field_length) + 1;
}
} // namespace SimpleDB


#endif
