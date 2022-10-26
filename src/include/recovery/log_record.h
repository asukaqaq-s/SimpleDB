#ifndef LOG_RECORD_H
#define LOG_RECORD_H

#include "file/block_id.h"
#include "config/config.h"
#include "config/type.h"
#include "record/tuple.h"
#include "file/page.h"

#include <cstring>
#include <sstream>
#include <string>

namespace SimpleDB {

class Transaction;
/**
* @brief i use logical-physical logrecord scheme instead of 
* physical logrecord scheme which be used to SimpleDB1.0.
*/
enum class LogRecordType {
    INVALID = 0,
    // table heap related
    INSERT,          // insert a tuple to table
    DELETE,          // delete a tuple in table
    UPDATE,          // update a tuple
    INITPAGE,        // create a page
    // txn related
    CHECKPOINTBEGIN,
    CHECKPOINTEND,
    BEGIN,
    COMMIT,
    ABORT,
    TXNEND, // remove the txn from active txn table
};

/**
* @brief
* LogRecord is a interface that each record implements it
* All operations on table such as Update、Insert、Delete, will be viewed as   
* several SetInt and SetString.
*
* LOG HEADER (record size will be automatically add by logmanager)
* -------------------------------------------------------
* | LSN | txn_ID | prevLSN | Logtype | IsCLR | UndoNext |
* -------------------------------------------------------
* the undo next is a lsn number, we only use it when undo phase
* so, only clr log need this undonext flag.otherwise, this flag is usually INVALID 
*
* For CheckPointBegin, InitPage, begin, commit, abort
* ----------
* | Header | 
* ----------
* For Insert
* -----------------------------------------------------------------------
* | Header | FileName.size() | FileName | RID | Tuple size | Tuple Data |
* -----------------------------------------------------------------------
* For delete type(including markdelete, rollbackdelete, applydelete)
* -----------------------------------------------------------------------
* | Header | FileName.size() | FileName | RID | Tuple size | Tuple Data |
* -----------------------------------------------------------------------
* For update type log record
* -----------------------------------------------------------------------------------------------------------------
* | Header | FileName.size() | FileName | RID | Old Tuple size | Old Tuple Data | New Tuple size | New Tuple Data |
* -----------------------------------------------------------------------------------------------------------------
* For init page type log record, we will change the size of table file in redo or undo phase
* -----------------------------------------------------
* | Header | FileName.size() | FileName | BlockNumber |
* -----------------------------------------------------
* For CheckPointEnd Log Record
* --------------------------------------------------------------------------------------------------
* | Header | txn map size | txn map data(char [] array) | dp map size | dp map data(char [] array) |
* --------------------------------------------------------------------------------------------------
*
* NOTE THAT, lsn、prev_lsn、undo_next_lsn will be automatically added by logmanager
* instead of recoverymanager. 
*/
class LogRecord {

public:

    LogRecord() = default;
    // virtual ~LogRecord() = default;

    inline void SetLsn(lsn_t lsn) {
        lsn_ = lsn;    
    }

    inline lsn_t GetLsn() {
        return lsn_;
    }

    inline void SetTxnID(txn_id_t txn) {
        txn_id_ = txn;
    }

    inline txn_id_t GetTxnID() {
        return txn_id_;
    }
    
    inline LogRecordType GetRecordType() {
        return type_;
    }

    inline void SetPrevLSN(lsn_t lsn) {
        prev_lsn_ = lsn;
    }

    inline lsn_t GetPrevLSN() {
        return prev_lsn_;
    }
    
    inline void SetCLR(bool flag) {
        is_clr_ = flag;
    }
    
    inline bool IsCLR() {
        return is_clr_;
    }

    inline void SetUndoNext(lsn_t lsn) {
        SIMPLEDB_ASSERT(is_clr_ == true, "should be used when clr log");
        undo_next_ = lsn;
    }

    inline lsn_t GetUndoNext() {
        return undo_next_;
    }
    
    std::string GetHeaderToString() {
        std::string clr;
        if(is_clr_) 
            clr = "CLR";
        std::stringstream os;
        os << "Log ["
           << "lsn: " << lsn_ << " "
           << "txnID: " << txn_id_ << " "
           << "prevLSN: " << prev_lsn_ << " "
           << "LogRecordType: " << static_cast<int>(type_) <<" "
           << "clr: " << clr << " " 
           << "undo_next: " << undo_next_ <<"]";

        return os.str();
    }
        
    virtual int RecordSize() = 0;

    virtual std::string ToString() = 0;

    /**
    * @brief 
    */
    virtual void Undo(Transaction *txn, lsn_t lsn) = 0;
    
    /**
    * @brief 
    */
    virtual void Redo(Transaction *txn) = 0;

    /**
    * @brief turn a logrecord object into the byte-sequence
    * 
    * @return byte-sequence
    */
    virtual std::shared_ptr<std::vector<char>> Serializeto() = 0;

    /**
    * @brief turn byte-sequence into a logrecord
    * 
    * @param byte_array a byte array
    */
    static std::shared_ptr<LogRecord> DeserializeFrom(const std::vector<char> &byte_array);

    
public: 

    // helper variable
    static constexpr uint32_t HEADER_SIZE = 
         // ------------------------------------------------------------------------------------------------------
         // | lsn           | txn_id_          | prev_lsn      | record_type           | is_clr      | undo_next |
         // ------------------------------------------------------------------------------------------------------
            sizeof(lsn_t) + sizeof(txn_id_t) + sizeof(lsn_t) + sizeof(LogRecordType) + sizeof(int) + sizeof(lsn_t);

protected: 

    /**
    * @brief helper function which build a page with log header
    * 
    * @return a built page
    */
    Page GetHeaderPage(int record_size);

    /**
    * @brief helper function which get header information through page
    */
    void GetHeaderInfor(Page *p);

protected: /* can be accessed by child class, can not be access by other class */
    

    // the corresponding lsn number which generated by logmanager
    lsn_t lsn_{INVALID_LSN};
    // the correspoding transaction_id
    txn_id_t txn_id_{INVALID_TXN_ID};
    // Used to construct a lsn_list
    lsn_t prev_lsn_{INVALID_LSN};
    // LogRecord Type
    LogRecordType type_;
    // is CLR_record ?
    int is_clr_{false};
    // be used in undo phase
    lsn_t undo_next_{INVALID_LSN};
};

//////////////////////////////////////////// Begin Checkpoint record //////////////////////////////////////////////////////

class ChkptBeginRecord : public LogRecord {

public:

    ChkptBeginRecord() : LogRecord() { type_ = LogRecordType::CHECKPOINTBEGIN; }
    
    ChkptBeginRecord(Page *p) { 
        GetHeaderInfor(p);
        assert(type_ == LogRecordType::CHECKPOINTBEGIN);
    }
    
    int RecordSize() override {
        return HEADER_SIZE;
    }

    void Redo(Transaction *txn) override { /* do nothing */ }
    
    void Undo(Transaction *txn, lsn_t lsn) override { /* do nothing */ }

    std::string ToString() override { return GetHeaderToString(); }

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const ChkptBeginRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               is_clr_ == obj.is_clr_ && 
               undo_next_ == obj.undo_next_;
    }
    
    bool operator !=(const ChkptBeginRecord &obj) const {
        return !(obj == *this);
    }

private:
    // nothing
};

enum TxStatus { U,C };

class TxTableEntry {
public:
    
    static_assert(sizeof(TxStatus) == sizeof(int));

    TxTableEntry() = default;

    TxTableEntry(lsn_t lsn, TxStatus stat) :
        last_lsn_(lsn), status_(stat) {}

    bool operator < (const TxTableEntry obj) const {
        if (obj.last_lsn_ == last_lsn_)
            return status_ < obj.status_;
        return last_lsn_ < obj.last_lsn_;
    }

    bool operator > (const TxTableEntry obj) const {
        if (obj.last_lsn_ == last_lsn_)
            return status_ > obj.status_;
        return last_lsn_ > obj.last_lsn_;
    }

    bool operator >= (const TxTableEntry obj) const {
        if (obj.last_lsn_ == last_lsn_)
            return status_ >= obj.status_;
        return last_lsn_ >= obj.last_lsn_;
    }

    bool operator <= (const TxTableEntry obj) const {
        if (obj.last_lsn_ == last_lsn_)
            return status_ <= obj.status_;
        return last_lsn_ <= obj.last_lsn_;
    }

    bool operator != (const TxTableEntry obj) const {
        return obj.last_lsn_ != last_lsn_ ||
               status_ != obj.status_;
    }

    bool operator == (const TxTableEntry obj) const {
        return obj.last_lsn_ == last_lsn_ &&
               status_ == obj.status_; 
    }
    
    lsn_t last_lsn_{INVALID_LSN};
    TxStatus status_{TxStatus::U};
};

class ChkptEndRecord : public LogRecord {

public:

    ChkptEndRecord(std::map<txn_id_t, TxTableEntry> tx_table,
                   std::map<BlockId, lsn_t> dp_table);
    
    ChkptEndRecord(Page *p);

    int RecordSize() override;

    void Redo(Transaction *txn) override { /* do nothing */ }
    
    void Undo(Transaction *txn, lsn_t lsn) override { /* do nothing */}

    std::map<txn_id_t, TxTableEntry> GetTxnTable() {
        return txn_table_;
    }

    std::map<BlockId, lsn_t> GetDPTable() {
        return dp_table_;
    }

    std::string ToString() override;

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const ChkptEndRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               is_clr_ == obj.is_clr_ && 
               undo_next_ == obj.undo_next_ &&
               txn_table_ == obj.txn_table_ &&
               dp_table_ == obj.dp_table_;
    }
    
    bool operator !=(const ChkptEndRecord &obj) const {
        return !(obj == *this);
    }

private:

    /**
    * @brief will be used by tostring method
    */
    std::string TxTableToString();

    /**
    * @brief will be used by tostring method
    */
    std::string DPTableToString();

    // avtive txn table
    // map txn_id to txn_tle
    std::map<txn_id_t, TxTableEntry> txn_table_;

    // dirty page table
    // map blockid to last lsn
    std::map<BlockId, lsn_t> dp_table_;
};


//////////////////////////////////////////// End Checkpoint record //////////////////////////////////////////////////////

//////////////////////////////////////////// Begin Transaction record ///////////////////////////////////////////////////

class BeginRecord : public LogRecord {

public:
    
    BeginRecord() = default;

    BeginRecord(txn_id_t txn_id) : LogRecord() { 
        txn_id_ = txn_id;
        type_ = LogRecordType::BEGIN; 
    }
    
    BeginRecord(Page *p) {
        GetHeaderInfor(p);
        assert(type_ == LogRecordType::BEGIN);
    }
    
    int RecordSize() override {
        return HEADER_SIZE;
    }

    void Redo(Transaction *txn) override { /* do nothing */ }
    
    void Undo(Transaction *txn, lsn_t lsn) override { /* do nothing */}

    std::string ToString() override { return GetHeaderToString(); }

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const BeginRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_;
    }
    
    bool operator !=(const BeginRecord &obj) const {
        return !(obj == *this);
    }

private:
    // nothing
};


class CommitRecord : public LogRecord {

public:

    CommitRecord() = default;
    
    CommitRecord(Page *p) { 
        GetHeaderInfor(p); 
        assert(type_ == LogRecordType::COMMIT);
    }

    CommitRecord(txn_id_t txn_id) { 
        txn_id_ = txn_id;
        type_ = LogRecordType::COMMIT;
    }
    
    int RecordSize() override {
        return HEADER_SIZE;
    }

    void Redo(Transaction *txn) override { /* do nothing */ }
    
    void Undo(Transaction *txn, lsn_t lsn) override { /* do nothing */}

    std::string ToString() override { return GetHeaderToString(); }

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const CommitRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_;
    }
    
    bool operator !=(const CommitRecord &obj) const {
        return !(obj == *this);
    }

private:
    // nothing
};



class AbortRecord : public LogRecord {
    
public:
    
    AbortRecord() = default;

    AbortRecord(txn_id_t txn_id) : LogRecord() { 
        txn_id_ = txn_id;
        type_ = LogRecordType::ABORT; 
    }
    
    AbortRecord(Page *p) {
        GetHeaderInfor(p);
        assert(type_ == LogRecordType::ABORT);
    }
    
    int RecordSize() override {
        return HEADER_SIZE;
    }

    void Redo(Transaction *txn) override { /* do nothing */ }
    
    void Undo(Transaction *txn, lsn_t lsn) override { /* do nothing */}

    std::string ToString() override { return GetHeaderToString(); }

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const AbortRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_;
    }
    
    bool operator !=(const AbortRecord &obj) const {
        return !(obj == *this);
    }

private:
    // nothing

};

class TxnEndRecord : public LogRecord {
    
public:
    
    TxnEndRecord() = default;

    TxnEndRecord(txn_id_t txn_id) : LogRecord() { 
        txn_id_ = txn_id;
        type_ = LogRecordType::TXNEND; 
    }
    
    TxnEndRecord(Page *p) {
        GetHeaderInfor(p);
        assert(type_ == LogRecordType::TXNEND);
    }
    
    int RecordSize() override {
        return HEADER_SIZE;
    }

    void Redo(Transaction *txn) override { /* do nothing */ }
    
    void Undo(Transaction *txn, lsn_t lsn) override { /* do nothing */}

    std::string ToString() override { return GetHeaderToString(); }

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const TxnEndRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_;
    }
    
    bool operator !=(const TxnEndRecord &obj) const {
        return !(obj == *this);
    }

private:
    // nothing

};

//////////////////////////////////////////// End Transaction record ///////////////////////////////////////////////////

//////////////////////////////////////////// Begin Table record ///////////////////////////////////////////////////////

/*
INSERT,          
DELETE
UPDATE,          
INITPAGE,
*/

class InsertRecord : public LogRecord {

public:

    InsertRecord() = default;
    
    InsertRecord(Page *p);

    InsertRecord(txn_id_t txn, 
                 const std::string &file_name, 
                 const RID &rid,
                 const Tuple &tuple);
    
    int RecordSize() override {
        return record_size_;
    }

    void Redo(Transaction *txn) override;

    void Undo(Transaction *txn, lsn_t lsn) override;

    std::string ToString() override;

    std::shared_ptr<std::vector<char>> Serializeto() override;

    inline std::string GetFileName() {
        return file_name_;
    }

    inline RID GetRID() {
        return rid_;
    }

    inline Tuple GetTuple() {
        return tuple_;
    }


    // for debugging purpose
    bool operator ==(const InsertRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               file_name_ == obj.file_name_ &&
               rid_ == obj.rid_ && 
               record_size_ == obj.record_size_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_  &&
               tuple_ == obj.tuple_;
    }
    
    bool operator !=(const InsertRecord &obj) const {
        return !(obj == *this);
    }

private:

    std::string file_name_;

    RID rid_;

    Tuple tuple_;

    int record_size_;
};


class DeleteRecord : public LogRecord {

public:

    DeleteRecord() = default;
    
    DeleteRecord(Page *p);

    DeleteRecord(txn_id_t txn, 
                 const std::string &file_name, 
                 const RID &rid,
                 const Tuple &tuple);
    
    int RecordSize() override {
        return record_size_;
    }

    void Redo(Transaction *txn) override;

    void Undo(Transaction *txn, lsn_t lsn) override;

    std::string ToString() override;

    std::shared_ptr<std::vector<char>> Serializeto() override;

    inline std::string GetFileName() {
        return file_name_;
    }

    inline RID GetRID() {
        return rid_;
    }

    inline Tuple GetTuple() {
        return tuple_;
    }

    // for debugging purpose
    bool operator ==(const DeleteRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               file_name_ == obj.file_name_ &&
               rid_ == obj.rid_ && 
               record_size_ == obj.record_size_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_  &&
               tuple_ == obj.tuple_;
    }
    
    bool operator !=(const DeleteRecord &obj) const {
        return !(obj == *this);
    }

private:

    std::string file_name_;

    RID rid_;

    Tuple tuple_;

    int record_size_;
};


class UpdateRecord : public LogRecord {

public:

    UpdateRecord() = default;
    
    UpdateRecord(Page *p);

    UpdateRecord(txn_id_t txn, 
                 const std::string &file_name, 
                 const RID &rid,
                 const Tuple &old_tuple,
                 const Tuple &new_tuple);
    
    int RecordSize() override {
        return record_size_;
    }

    void Redo(Transaction *txn) override;

    void Undo(Transaction *txn, lsn_t lsn) override;

    std::string ToString() override;

    std::shared_ptr<std::vector<char>> Serializeto() override;


    inline std::string GetFileName() {
        return file_name_;
    }

    inline RID GetRID() {
        return rid_;
    }

    inline Tuple GetNewTuple() {
        return new_tuple_;
    }

    inline Tuple GetOldTuple() {
        return old_tuple_;
    }

    // for debugging purpose
    bool operator ==(const UpdateRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               file_name_ == obj.file_name_ &&
               rid_ == obj.rid_ && 
               record_size_ == obj.record_size_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_  &&
               old_tuple_ == obj.old_tuple_ &&
               new_tuple_ == obj.new_tuple_;
    }
    
    bool operator !=(const UpdateRecord &obj) const {
        return !(obj == *this);
    }

private:

    std::string file_name_;

    RID rid_;

    Tuple old_tuple_;

    Tuple new_tuple_;

    int record_size_;
};

class InitPageRecord : public LogRecord {

public:

    InitPageRecord() = default;
    
    InitPageRecord(Page *p);

    InitPageRecord(txn_id_t txn, 
                   const std::string &file_name, 
                   int block_number);
    
    int RecordSize() override {
        return record_size_;
    }

    BlockId GetBlockID() {
        return BlockId(file_name_, block_number_);
    }

    void Redo(Transaction *txn) override;

    void Undo(Transaction *txn, lsn_t lsn) override;

    std::string ToString() override;

    std::shared_ptr<std::vector<char>> Serializeto() override;

    // for debugging purpose
    bool operator ==(const InitPageRecord &obj) const {
        return lsn_ == obj.lsn_ && 
               txn_id_ == obj.txn_id_ && 
               prev_lsn_ == obj.prev_lsn_ && 
               file_name_ == obj.file_name_ &&
               record_size_ == obj.record_size_ && 
               is_clr_ == obj.is_clr_ &&
               undo_next_ == obj.undo_next_  &&
               block_number_ == obj.block_number_;
    }
    
    bool operator !=(const InitPageRecord &obj) const {
        return !(obj == *this);
    }

private:

    std::string file_name_;

    int block_number_;

    int record_size_;
};




} // namespace SimpleDB

#endif