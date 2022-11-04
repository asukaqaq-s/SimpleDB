#ifndef BUFFER_CC
#define BUFFER_CC

#include "buffer/buffer.h"
#include "file/file_manager.h"
#include "recovery/recovery_manager.h"

namespace SimpleDB {

Buffer::Buffer(FileManager *file_manager) {
    data_ = std::make_unique<Page> (file_manager->BlockSize());           
}

void Buffer::AssignBlock(BlockId blk, FileManager *file_manager, RecoveryManager *recovery_manager) {
    block_ = blk;
    file_manager->Read(block_, *data_);
    pin_count_ = 0;
}

void Buffer::SetPageLsn(lsn_t lsn) {
    if (lsn > INVALID_LSN) {
        SIMPLEDB_ASSERT(lsn >= data_->GetLsn(), "lsn error");
        data_->SetLsn(lsn);
        is_dirty_ = true;
    }
}


lsn_t Buffer::GetPageLsn() const {
    return data_->GetLsn();
}


void Buffer::flush(FileManager *file_manager, RecoveryManager *recovery_manager) {
    // because of WAL protocol, we should flush relative log record
    // before writing block to disk.
    if (is_dirty_) {
        recovery_manager->FlushBlock(block_, data_->GetLsn());
        file_manager->Write(block_, *data_);
        is_dirty_ = false;
        
        // reset content
        data_ = std::make_unique<Page> (data_->GetSize());
    }
}

} // namespace SimpleDB


#endif