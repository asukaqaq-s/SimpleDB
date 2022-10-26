#ifndef CHECKPOINT_END_RECORD_CC
#define CHECKPOINT_END_RECORD_CC

#include "recovery/log_record.h"

namespace SimpleDB {

// a txn_map_entry = integer + tx_table_entry
// entry format:
// | int | tx_table_entry.lsn | tx_table_entry.status |
// the size of entry is constant


// a dp_map_entry = blockid + lsn_t
// entry format:
// | filename size | filename | int | lsn_t |
// the size of entry is not constant, we should dynamically calc


ChkptEndRecord::ChkptEndRecord(std::map<txn_id_t, TxTableEntry> tx_table,
                               std::map<BlockId, lsn_t> dp_table)
    : LogRecord(), txn_table_(tx_table), dp_table_(dp_table) {
    type_ = LogRecordType::CHECKPOINTEND;
}
    
ChkptEndRecord::ChkptEndRecord(Page *p) {
    GetHeaderInfor(p);
    SIMPLEDB_ASSERT(type_ == LogRecordType::CHECKPOINTEND, "");
    
    int addr = HEADER_SIZE;
    int txn_table_size = p->GetInt(addr);
    int txn_table_end = addr + txn_table_size;
    int txn_table_entry_size = sizeof(int) + sizeof(lsn_t) + sizeof(int);

    SIMPLEDB_ASSERT(txn_table_size % 4 == 0, "should not happen");

    addr += sizeof(int);
    for (;addr < txn_table_end; addr += txn_table_entry_size) {
        int offset = 0;
        int txn_id, tx_status;
        lsn_t last_lsn;
        
        txn_id = p->GetInt(addr + offset);
        offset += sizeof(int);
        last_lsn = p->GetInt(addr + offset);
        offset += sizeof(lsn_t);
        tx_status = p->GetInt(addr + offset);

        txn_table_[txn_id] = TxTableEntry(last_lsn, 
                             static_cast<TxStatus> (tx_status));
    } 
    
    int dp_table_size = p->GetInt(addr);
    int dp_table_end = addr + dp_table_size;
    int dp_table_entry_size;
    
    addr += sizeof(int);
    for (;addr < dp_table_end; addr += dp_table_entry_size) {
        std::string file_name;
        int block_number;
        lsn_t first_lsn;
        dp_table_entry_size = 0;
        
        file_name = p->GetString(addr + dp_table_entry_size);
        dp_table_entry_size += Page::MaxLength(file_name.size());
        block_number = p->GetInt(addr + dp_table_entry_size);
        dp_table_entry_size += sizeof(int);
        first_lsn = p->GetInt(addr + dp_table_entry_size);
        dp_table_entry_size += sizeof(lsn_t);
        
        dp_table_[BlockId(file_name, block_number)] = first_lsn;
    }
}

int ChkptEndRecord::RecordSize() {
    int txn_entry_size;
    int dp_entry_size;
    int txn_table_size;
    int dp_table_size;
    int record_size;

    // a txn_map_entry = integer + tx_table_entry
    // entry format:
    // | int | tx_table_entry.lsn | tx_table_entry.status |
    // the size of entry is constant
    txn_entry_size = sizeof(int) + sizeof(lsn_t) + sizeof(int);
    // sizeof(int) means byte-array'ssize
    txn_table_size = sizeof(int) + txn_entry_size * txn_table_.size();
    
    // a dp_map_entry = blockid + lsn_t
    // entry format:
    // | filename size | filename | int | lsn_t |
    // the size of entry is not constant, we should dynamically calc
    dp_entry_size = 0;
    dp_table_size = 0; 

    for (auto t:dp_table_) {
        dp_entry_size = Page::MaxLength(t.first.FileName().size()) +
                        sizeof(int) +
                        sizeof(lsn_t);
        dp_table_size += dp_entry_size;
    }

    dp_table_size += sizeof(int);

    record_size = HEADER_SIZE + txn_table_size + dp_table_size;
    return record_size;
}

std::string ChkptEndRecord::ToString() {
    std::string header = GetHeaderToString();
    header += "\n" + TxTableToString() + "\n" + DPTableToString();
    
    return header;
}

std::shared_ptr<std::vector<char>> ChkptEndRecord::Serializeto() {
    int record_size = RecordSize();
    auto p = GetHeaderPage(record_size);
    int addr = HEADER_SIZE;
    int txn_table_begin = addr;
    int dp_table_begin;

    addr += sizeof(int);
    for (auto t:txn_table_) {
        int offset = 0;
        int txn_id = t.first;
        lsn_t last_lsn = t.second.last_lsn_;
        TxStatus tx_status = t.second.status_;
        
        p.SetInt(addr + offset, txn_id);
        offset += sizeof(int);
        p.SetInt(addr + offset, last_lsn);
        offset += sizeof(lsn_t);
        p.SetInt(addr + offset, static_cast<int>(tx_status));
        offset += sizeof(int);

        addr += offset;
    }

    p.SetInt(txn_table_begin, addr - txn_table_begin - sizeof(int));
    
    dp_table_begin = addr;
    addr += sizeof(int);
    for (auto t:dp_table_) {
        int offset = 0;
        std::string file_name = t.first.FileName();
        int block_number = t.first.BlockNum();
        lsn_t lsn = t.second;

        p.SetString(addr + offset, file_name);
        offset += Page::MaxLength(file_name.size());
        p.SetInt(addr + offset, block_number);
        offset += sizeof(int);
        p.SetInt(addr + offset, lsn);
        offset += sizeof(lsn_t);

        addr += offset;
    }
    p.SetInt(dp_table_begin, addr - dp_table_begin - sizeof(int));

    SIMPLEDB_ASSERT(record_size == addr, "");
    return p.content();
}

std::string ChkptEndRecord::TxTableToString() {
    std::stringstream str;
    
    str << "{ ";
    
    for (auto t:txn_table_) {
        str << "\t[tx = " << t.first << " "
            << "last_lsn = " << t.second.last_lsn_ << " "
            << "tx_status = "<< t.second.status_ << "]," << std::endl;
    }
    str << " }";
    return str.str();
}


std::string ChkptEndRecord::DPTableToString() {
    std::stringstream str;
    
    str << "{ ";
    
    for (auto t:dp_table_) {
        str << "\t[file = " << t.first.FileName() << " "
            << "blocknum = " << t.first.BlockNum() << " "
            << "lsn = "<< t.second << "]," << std::endl;
    }
    str << " }";
    return str.str();
}



} // namespace SimpleDB

#endif