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
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace SimpleDB {


// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetLockStage(), LockStage::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetLockStage(), LockStage::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetTxnState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetTxnState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {

    int s_cnt = 0;
    int cnt = txn->GetLockSet()->size();
    for (auto t : *txn->GetLockSet()) {
        if (t.second == LockMode::SHARED) {
            s_cnt++;
        }
    }

    int e_cnt = cnt - s_cnt;

    EXPECT_EQ(s_cnt, shared_size);
    EXPECT_EQ(e_cnt, exclusive_size);
}

// --- Real tests ---
// Basic single thread upgrade test
void UpgradeTest() {

    //
    // prepare
    // 
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    auto lock_mgr = std::make_unique<LockManager> ();

    TransactionManager txn_mgr(std::move(lock_mgr), rm.get(), file_manager.get(), buf_manager.get());
    LockManager *lock = txn_mgr.GetLockManager();

    BlockId block{"123", 0};
    auto txn = txn_mgr.Begin();

    bool res = lock->LockShared(txn.get(), block);
    EXPECT_TRUE(res);
    CheckTxnLockSize(txn.get(), 1, 0);
    CheckGrowing(txn.get());

    res = lock->LockUpgrade(txn.get(), block);
    EXPECT_TRUE(res);
    CheckTxnLockSize(txn.get(), 0, 1);
    CheckGrowing(txn.get());
    txn_mgr.Commit(txn.get());

    EXPECT_TRUE(res);
    CheckTxnLockSize(txn.get(), 0, 0);
    CheckShrinking(txn.get());

    
    CheckCommitted(txn.get());
}

/****************************
 * Basic Tests (15 pts)
 ****************************/

const size_t NUM_ITERS = 10;

/*
 * Score 5
 * Description: test lock upgrade.
 */
TEST(LockManagerTest, UpgradeLockTest) {
  TEST_TIMEOUT_BEGIN
  for (size_t i = 0; i < NUM_ITERS; i++) {
    UpgradeTest();
  }
  TEST_TIMEOUT_FAIL_END(1000 * 30)
}



void BasicTest1() {
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    auto lock_mgr_ = std::make_unique<LockManager> ();

    TransactionManager txn_mgr(std::move(lock_mgr_), rm.get(), file_manager.get(), buf_manager.get());
    LockManager *lock_mgr = txn_mgr.GetLockManager();

    std::vector<BlockId> blockids;
    std::vector<std::unique_ptr<Transaction>> txns;

    int num_blockids = 10;
    for (int i = 0; i < num_blockids; i++) {
        BlockId block{test_file, static_cast<int>(i)};
        blockids.push_back(block);
        txns.emplace_back(txn_mgr.Begin());
        EXPECT_EQ(i, txns[i]->GetTxnID());
    }
    // test

    auto task = [&](int txn_id) {
        bool res;
        for (const BlockId &blockid : blockids) {
            res = lock_mgr->LockShared(txns[txn_id].get(), blockid);
            EXPECT_TRUE(res);
            CheckGrowing(txns[txn_id].get());
        }

        txn_mgr.Commit(txns[txn_id].get());
        CheckShrinking(txns[txn_id].get());
        CheckCommitted(txns[txn_id].get());
        CheckTxnLockSize(txns[txn_id].get(), 0, 0);
    };
    std::vector<std::thread> threads;
    threads.reserve(num_blockids);

    for (int i = 0; i < num_blockids; i++) {
        threads.emplace_back(std::thread{task, i});
    }

    for (int i = 0; i < num_blockids; i++) {
        threads[i].join();
    }


}
TEST(LockManagerTest, BasicTest) { BasicTest1(); }

void TwoPLTest() {
    
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);
    
    auto lock_mgr_ = std::make_unique<LockManager> ();
    
    TransactionManager txn_mgr(std::move(lock_mgr_), rm.get(), file_manager.get(), buf_manager.get());
    LockManager *lock_mgr = txn_mgr.GetLockManager();

    BlockId blockid0{test_file, 0};
    BlockId blockid1{test_file, 1};

    auto txn_ = txn_mgr.Begin();   
    auto txn = txn_.get();
    EXPECT_EQ(0, txn->GetTxnID());
    
    bool res;
    res = lock_mgr->LockShared(txn, blockid0);
    EXPECT_TRUE(res);
    CheckGrowing(txn);
    CheckTxnLockSize(txn, 1, 0);

    res = lock_mgr->LockExclusive(txn, blockid1);
    EXPECT_TRUE(res);
    CheckGrowing(txn);
    CheckTxnLockSize(txn, 1, 1);

    // Need to call txn_mgr's abort
    txn_mgr.Abort(txn);
    CheckAborted(txn);
    CheckTxnLockSize(txn, 0, 0);

}
TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }


void WoundWaitBasicTest() {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    auto lock_mgr_ = std::make_unique<LockManager> ();

    TransactionManager txn_mgr(std::move(lock_mgr_), rm.get(), file_manager.get(), buf_manager.get());
    LockManager *lock_mgr = txn_mgr.GetLockManager();


    BlockId blockid{test_file, 0};


    std::promise<void> t1done;
    std::shared_future<void> t1_future(t1done.get_future());

    auto wait_die_task = [&]() {
        // younger transaction acquires lock first
        auto txn_die_ = txn_mgr.Begin();
        auto txn_die = txn_die_.get();

        bool res = lock_mgr->LockExclusive(txn_die, blockid);
        EXPECT_TRUE(res);

        CheckGrowing(txn_die);
        CheckTxnLockSize(txn_die, 0, 1);

        t1done.set_value();

        // wait for txn 0 to call lock_exclusive(), which should wound us
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        CheckAborted(txn_die);

        // unlock
        txn_mgr.Abort(txn_die);
    };

    auto txn_hold_ = txn_mgr.Begin();
    auto txn_hold = txn_hold_.get();

    // launch the waiter thread
    std::thread wait_thread{wait_die_task};

    // wait for txn1 to lock
    t1_future.wait();

    bool res = lock_mgr->LockExclusive(txn_hold, blockid);
    EXPECT_TRUE(res);

    wait_thread.join();

    CheckGrowing(txn_hold);
    txn_mgr.Commit(txn_hold);
    CheckCommitted(txn_hold);
}

void WoundWaitTest() {
  char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    auto lock_mgr_ = std::make_unique<LockManager> ();

    TransactionManager txn_mgr(std::move(lock_mgr_), rm.get(), file_manager.get(), buf_manager.get());
    LockManager *lock_mgr = txn_mgr.GetLockManager();

    BlockId blockid0{test_file, 0};
    BlockId blockid1{test_file, 1};

    std::mutex id_mutex;
    size_t id_hold = 0;
    size_t id_wait = 10;
    size_t id_kill = 1;

    size_t num_wait = 10;
    size_t num_kill = 1;

    std::vector<std::thread> wait_threads;
    std::vector<std::thread> kill_threads;
    std::vector<std::unique_ptr<Transaction>> wait_txns;
    std::vector<std::unique_ptr<Transaction>> kill_txns;
    std::vector<std::unique_ptr<Transaction>> hold_txns;

    for (int i = 0;i < 10;i ++) {
        if (i == 1) {
            kill_txns.emplace_back(txn_mgr.Begin());
        }
        else if (i == 0) {
            hold_txns.emplace_back(txn_mgr.Begin());
        }
        else {
            auto tx = txn_mgr.Begin();
        }
    }


    for (int i = 0;i < 10;i ++) {
        wait_txns.emplace_back(txn_mgr.Begin());
    }

    
    auto txn = hold_txns[0].get();
    
    lock_mgr->LockExclusive(txn, blockid0);
    lock_mgr->LockShared(txn, blockid1);

    auto wait_die_task = [&]() {
        id_mutex.lock();
        Transaction* wait_txn = wait_txns[id_wait++ - 10].get();
        id_mutex.unlock();
        bool res;
        res = lock_mgr->LockShared(wait_txn, blockid1);
        EXPECT_TRUE(res);
        CheckGrowing(wait_txn);
        CheckTxnLockSize(wait_txn, 1, 0);

        try {
            res = lock_mgr->LockExclusive(wait_txn, blockid0);
            EXPECT_FALSE(res) << wait_txn->GetTxnID() << "ERR";
        } catch (const TransactionAbortException &e) {
        } catch (const Exception &e) {
            EXPECT_TRUE(false) << "Test encountered exception" << e.what();
        }

        CheckAborted(wait_txn);
        txn_mgr.Abort(wait_txn);
    };

    // All transaction here should wait.
    for (size_t i = 0; i < num_wait; i++) {
        wait_threads.emplace_back(std::thread{wait_die_task});
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // TODO(peijingx): guarantee all are waiting on LockExclusive
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto kill_task = [&]() {
        Transaction* kill_txn = kill_txns[id_kill++ - 1].get();
        bool res;
        res = lock_mgr->LockShared(kill_txn, blockid1);
        EXPECT_TRUE(res);
        CheckGrowing(kill_txn);
        CheckTxnLockSize(kill_txn, 1, 0);

        res = lock_mgr->LockShared(kill_txn, blockid0);
        EXPECT_TRUE(res);
        CheckGrowing(kill_txn);
        CheckTxnLockSize(kill_txn, 2, 0);

        txn_mgr.Commit(kill_txn);
        CheckCommitted(kill_txn);
        CheckTxnLockSize(kill_txn, 0, 0);
    };

    for (size_t i = 0; i < num_kill; i++) {
        kill_threads.emplace_back(std::thread{kill_task});
    }

    for (size_t i = 0; i < num_wait; i++) {
        wait_threads[i].join();
    }

    CheckGrowing(txn);
    txn_mgr.Commit(txn);
    CheckCommitted(txn);

    for (size_t i = 0; i < num_kill; i++) {
        kill_threads[i].join();
    }
}






TEST(LockManagerTest, WoundWaitBasicTest) { WoundWaitBasicTest(); }


TEST(LockManagerTest, WoundWaitTest) { WoundWaitTest();}






}  // namespace SimpleDB


