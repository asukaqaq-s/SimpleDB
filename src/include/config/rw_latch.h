#ifndef RW_LATCH_H
#define RW_LATCH_H

#include <mutex>  // NOLINT
#include <condition_variable>
#include <climits>
#include <shared_mutex>

namespace SimpleDB {

class ReaderWriterLatch {
    const uint32_t MAX_READERS = UINT_MAX;

public:

    ReaderWriterLatch() = default;
    ~ReaderWriterLatch() = default;
    // can not copy constructor  
    ReaderWriterLatch(const ReaderWriterLatch &) = delete;
    ReaderWriterLatch &operator=(const ReaderWriterLatch &) = delete;


    /**
    * @brief
    * acquire writer latch
    */
    void WLock() {
        mutex_.lock();
    }

    /**
     * @brief 
     * release writer latch
     */
    void WUnlock() {
        mutex_.unlock();
    }

    /**
    * @brief 
    * acquire reader latch
    */
    void RLock() {  
        mutex_.lock_shared();
    }

    void RUnlock() {
        mutex_.unlock_shared();
    }

private:
    
    std::shared_mutex mutex_;
};

class ReaderGuard {
public:
    ReaderGuard(ReaderWriterLatch &latch): latch_(latch) {
        latch_.RLock();
    }
    ~ReaderGuard() {
        latch_.RUnlock();
    }
private:
    ReaderWriterLatch &latch_;
};

class WriterGuard {
public:
    WriterGuard(ReaderWriterLatch &latch): latch_(latch) {
        latch_.WLock();
    }
    ~WriterGuard() {
        latch_.WUnlock();
    }
private:
    ReaderWriterLatch &latch_;
};

} // namespace SimpleDB

#endif