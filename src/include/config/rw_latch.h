#ifndef RW_LATCH_H
#define RW_LATCH_H

#include <mutex>  // NOLINT
#include <condition_variable>
#include <climits>

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
        std::unique_lock<std::mutex> latch(latch_);
        if (writer_entered_) {
            // wait as a reader, will notify when release writer latch
            reader_.wait(latch, [&]() {return !writer_entered_;});
        }
        
        writer_entered_ = true;
        if (reader_count_ > 0) {
            // wait as writer, will notify when release reader latch
            writer_.wait(latch, [&]() {return reader_count_ == 0; });
        }
    }

    /**
     * @brief 
     * release writer latch
     */
    void WUnlock() {
        std::unique_lock<std::mutex> latch(latch_);
        writer_entered_ = false;

        // notify all readers
        reader_.notify_all();
    }

    /**
    * @brief 
    * acquire reader latch
    */
    void RLock() {  
        std::unique_lock<std::mutex> latch(latch_);
        if (writer_entered_ || reader_count_ == MAX_READERS) {
            reader_.wait(latch, [&]() {
                return writer_entered_ = false && reader_count_ < MAX_READERS;
            });
        }

        reader_count_ ++;
    }

    void RUnlock() {
        std::unique_lock<std::mutex> latch(latch_);
        reader_count_ --;
        if (writer_entered_) {
            if (reader_count_ == 0) {
                // just notify one, otherwise will lost
                // writer acquire
                writer_.notify_one();
            }
        } else {
            // allow oone more reader
            if (reader_count_ == MAX_READERS - 1) {
                reader_.notify_one();
            }
        }
    }

private:
    
    std::mutex latch_;
    std::condition_variable writer_;
    std::condition_variable reader_;
    uint32_t reader_count_{0};
    bool writer_entered_{false};
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