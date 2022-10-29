/**
 * grading_lock_manager_test_1.cpp
 */

#include <atomic>
#include <future>  //NOLINT
#include <random>
#include <thread>  //NOLINT

#include "config/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/lock_manager.h"
#include "gtest/gtest.h"

namespace SimpleDB {
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

const std::string test_file = "testfile.txt";
std::string directory_name;


void Init() {
    char buf[100];
    directory_name = getcwd(buf, 100);
    directory_name += "/test_dir";    
}

int txn_cnt = 1;

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetLockStage(), LockStage::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetLockStage(), LockStage::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetTxnState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetTxnState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, int shared_size, int exclusive_size) {
    
    int shared_size_ = 0;
    int exclusive_size_ = txn->GetLockSet()->size();

    for (auto t: *txn->GetLockSet()) {
        if (t.second == LockMode::SHARED) {
            shared_size_++;
        }
    }

    exclusive_size_ -= shared_size_;

    EXPECT_EQ(shared_size_, shared_size);
    EXPECT_EQ(exclusive_size_, exclusive_size);
}

// --- Real tests ---
// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
    LockManager lock_mgr{};
    FileManager fm(directory_name, 4096);
    LogManager lm(&fm, "log.txt");
    RecoveryManager rm(&lm);
    BufferManager bm(&fm, &rm, 100);

    std::vector<BlockId> blockids;
    std::vector<Transaction *> txns;
    int num_blockids = 10;
    for (int i = 0; i < num_blockids; i++) {
        BlockId blk(test_file, i);
        blockids.push_back(blk);
        txns.push_back(new Transaction(&fm, &bm, &rm));
        EXPECT_EQ(txn_cnt ++, txns[i]->GetTxnID());
    }
    // test

    auto task = [&](int txn_id) {
        bool res;
        for (const BlockId &blockid : blockids) {
            res = lock_mgr.LockShared(txns[txn_id], blockid);
            EXPECT_TRUE(res);
            CheckGrowing(txns[txn_id]);
        }

        txns[txn_id]->SetLockStage(LockStage::SHRINKING);

        for (const BlockId &blockid : blockids) {
            res = lock_mgr.UnLock(txns[txn_id], blockid);
            EXPECT_TRUE(res);
            CheckShrinking(txns[txn_id]);
        }

        txns[txn_id]->Commit();
        CheckCommitted(txns[txn_id]);
    };

    std::vector<std::thread> threads;
    threads.reserve(num_blockids);

    for (int i = 0; i < num_blockids; i++) {
        threads.emplace_back(std::thread{task, i});
    }

    for (int i = 0; i < num_blockids; i++) {
        threads[i].join();
    }

    for (int i = 0; i < num_blockids; i++) {
        delete txns[i];
    }
}




void WoundWaitBasicTest() {
    BlockId block0(test_file, 0);
    BlockId block1(test_file, 1);

    FileManager fm(directory_name, 4096);
    LogManager lm(&fm, "log.txt");
    RecoveryManager rm(&lm);
    BufferManager bm(&fm, &rm, 100);


    std::mutex id_mutex;
    int id_hold = 0;
    int id_wait = 10;
    int id_kill = 1;

    int num_wait = 10;
    int num_kill = 1;

    std::vector<std::thread> wait_threads;
    std::vector<std::thread> kill_threads;

    std::shared_ptr<Transaction> txn = std::make_shared<Transaction>(&fm, &bm, &rm, id_hold);
    
    txn->AcquireLock(block0, LockMode::EXCLUSIVE);
    txn->AcquireLock(block1, LockMode::SHARED);

    auto wait_die_task = [&]() {
        
        Transaction wait_txn(&fm, &bm, &rm, id_wait++);
        
        wait_txn.AcquireLock(block1, LockMode::SHARED);
        
        CheckGrowing(&wait_txn);
        CheckTxnLockSize(&wait_txn, 1, 0);
        
        try {
            wait_txn.AcquireLock(block0, LockMode::EXCLUSIVE);
        } catch (const TransactionAbortException &e) {
        } catch (const Exception &e) {
            EXPECT_TRUE(false) << "Test encountered exception" << e.what();
        }

        CheckAborted(&wait_txn);

        wait_txn.Abort();
    };

    // All transaction here should wait.
    for (int i = 0; i < num_wait; i++) {
        wait_threads.emplace_back(std::thread{wait_die_task});
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // TODO(peijingx): guarantee all are waiting on LockExclusive
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto kill_task = [&]() {
        Transaction kill_txn(&fm, &bm, &rm, id_kill++);
        
        bool res;
        kill_txn.AcquireLock(block1, LockMode::SHARED);
        CheckGrowing(&kill_txn);
        CheckTxnLockSize(&kill_txn, 1, 0);

        kill_txn.AcquireLock(block0, LockMode::SHARED);
        CheckGrowing(&kill_txn);
        CheckTxnLockSize(&kill_txn, 2, 0);

        kill_txn.Commit();
        CheckCommitted(&kill_txn);
        CheckTxnLockSize(&kill_txn, 0, 0);
    };

    
    for (size_t i = 0; i < num_kill; i++) {
        kill_threads.emplace_back(std::thread{kill_task});
    }

    for (size_t i = 0; i < num_wait; i++) {
        wait_threads[i].join();
    }

    CheckGrowing(txn.get());
    txn->Commit();
    CheckCommitted(txn.get());

    for (size_t i = 0; i < num_kill; i++) {
        kill_threads[i].join();
    }
    return;

    
}


void WoundWaitDeadlockTest() {
    BlockId block0(test_file, 0);
    BlockId block1(test_file, 1);

    FileManager fm(directory_name, 4096);
    LogManager lm(&fm, "log.txt");
    RecoveryManager rm(&lm);
    BufferManager bm(&fm, &rm, 100);
    Transaction txn1(&fm, &bm, &rm, 1);

    bool res;
    txn1.AcquireLock(block0, LockMode::EXCLUSIVE);
    
    CheckGrowing(&txn1);
    // This will imediately take the lock

    // Use promise and future to identify
    auto task = [&](std::promise<void> lock_acquired) {
        Transaction txn0(&fm, &bm, &rm, 0);  // this transaction is older than txn1
        txn1.AcquireLock(block0, LockMode::EXCLUSIVE);
        CheckGrowing(&txn0);
        txn0.AcquireLock(block0, LockMode::EXCLUSIVE);

        CheckGrowing(&txn0);
        txn0.Commit();

        lock_acquired.set_value();
    };

    std::promise<void> lock_acquired;
    std::future<void> lock_acquired_future = lock_acquired.get_future();

    std::thread t{task, std::move(lock_acquired)};
    auto otask = [&](Transaction *tx) {
        while (tx->GetTxnState() != TransactionState::ABORTED) {
        }
        tx->Abort();
    };
    std::thread w{otask, &txn1};
    lock_acquired_future.wait();  // waiting for the thread to acquire exclusive lock for block1
    // this should fail, and txn abort and release all the locks.

    t.join();
    w.join();
}

// // Large number of lock and unlock operations.
// void WoundWaitStressTest() {
//     BlockId block0(test_file, 0);
//     BlockId block1(test_file, 1);

//     FileManager fm(directory_name, 4096);
//     LogManager lm(&fm, "log.txt");
//     RecoveryManager rm(&lm);
//     BufferManager bm(&fm, &rm, 100);

//     std::mt19937 generator(time(nullptr));

//     size_t num_blocks = 100;
//     size_t num_threads = 1000;

//     std::vector<BlockId> blocks;
//     for (uint32_t i = 0; i < num_blocks; i++) {
//         BlockId block(test_file, i);
//         blocks.push_back(block);
//     }

//     // Task1 is to get shared lock until abort
//     auto task1 = [&](int tid) {
//         Transaction txn(&fm, &bm, &rm, tid);
//         int num_shared = 0;
//         int mod = 2;
//         for (size_t i = 0; i < blocks.size(); i++) {
//             if (i % mod == 0) {
//                 txn.AcquireLock(blocks[i], LockMode::SHARED);
//                 num_shared++;
//                 CheckTxnLockSize(&txn, num_shared, 0);
//             }
//         }


//         CheckGrowing(&txn);

//         if (txn.IsAborted()) {
//             txn.Abort();
//         } else {
//             txn.Commit();
//         }
        
//         CheckTxnLockSize(&txn, 0, 0);
//     };

//     // Task 2 is shared lock from the back
//     auto task2 = [&](int tid) {
//         Transaction txn(&fm, &bm, &rm, tid);
//         int mod = 3;
//         int num_shared = 0;
//         for (int i = static_cast<int>(blocks.size()) - 1; i >= 0; i--) {
//             if (i % mod == 0) {
//                 txn.AcquireLock(blocks[i], LockMode::EXCLUSIVE);
//                 num_shared++;
//                 CheckTxnLockSize(&txn, num_shared, 0);
//             }
//         }
//         CheckGrowing(&txn);
        
//         if (txn.IsAborted()) {
//             txn.Abort();
//         } else {
//             txn.Commit();
//         }
        
//         CheckTxnLockSize(&txn, 0, 0);
//     };

//     // Shared lock wants to upgrade
//     auto task3 = [&](int tid) {
//         Transaction txn(tid);
//         int num_exclusive = 0;
//         int mod = 6;
//         bool res;
//         for (size_t i = 0; i < blocks.size(); i++) {
//         if (i % mod == 0) {
//             res = AcquireLock(&txn, blocks[i]);
//             if (!res) {
//             CheckTxnLockSize(&txn, 0, num_exclusive);
//             CheckAborted(&txn);
//             txn_mgr.Abort(&txn);
//             CheckTxnLockSize(&txn, 0, 0);
//             return;
//             }
//             CheckTxnLockSize(&txn, 1, num_exclusive);
//             res = lock_mgr.LockUpgrade(&txn, blocks[i]);
//             if (!res) {
//             CheckAborted(&txn);
//             txn_mgr.Abort(&txn);
//             CheckTxnLockSize(&txn, 0, 0);
//             return;
//             }
//             num_exclusive++;
//             CheckTxnLockSize(&txn, 0, num_exclusive);
//             CheckGrowing(&txn);
//         }
//         }
//         for (size_t i = 0; i < blocks.size(); i++) {
//         if (i % mod == 0) {
//             res = lock_mgr.Unlock(&txn, blocks[i]);

//             EXPECT_TRUE(res);
//             CheckShrinking(&txn);
//             // A fresh BlockId here
//             BlockId block{tid, static_cast<uint32_t>(tid)};
//             res = AcquireLock(&txn, block);
//             EXPECT_FALSE(res);

//             CheckAborted(&txn);
//             txn_mgr.Abort(&txn);
//             CheckTxnLockSize(&txn, 0, 0);
//             return;
//         }
//         }
//     };

//     // Exclusive lock and unlock
//     auto task4 = [&](int tid) {
//         Transaction txn(tid);
//         // randomly pick
//         int index = static_cast<int>(generator() % blocks.size());
//         bool res = lock_mgr.LockExclusive(&txn, blocks[index]);
//         if (res) {
//         bool res = lock_mgr.Unlock(&txn, blocks[index]);
//         EXPECT_TRUE(res);
//         } else {
//             txn_mgr.Abort(&txn);
//         }
//         CheckTxnLockSize(&txn, 0, 0);
//     };

//     std::vector<std::function<void(int)>> tasks{task1, task2, task4};
//     std::vector<std::thread> threads;
//     // only one task3 to ensure success upgrade most of the time
//     threads.emplace_back(std::thread{task3, num_threads / 2});
//     for (size_t i = 0; i < num_threads; i++) {
//         if (i != num_threads / 2) {
//         threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
//         }
//     }
//     for (size_t i = 0; i < num_threads; i++) {
//         // Threads might be returned already
//         if (threads[i].joinable()) {
//         threads[i].join();
//         }
//     }
// }





// Correct case

/****************************
 * Basic Tests (15 pts)
 ****************************/

const int NUM_ITERS = 10;

/*
 * Score: 5
 * Description: Basic tests for LockShared and UnLock operations
 * on small amount of blockids.
 */
TEST(LockManagerTest, BasicTest) {

    Init();return;
  TEST_TIMEOUT_BEGIN
  for (int i = 0; i < NUM_ITERS; i++) {
    BasicTest1();
  }
  TEST_TIMEOUT_FAIL_END(1000 * 30)
}

TEST(LockManagerTest, WoundWaitBasicTest) { 
    return;
    WoundWaitBasicTest();
}

TEST(LockManagerTest, WoundWaitDeadLockTest) { 
    WoundWaitDeadlockTest();
}


}  // namespace SimpleDB


