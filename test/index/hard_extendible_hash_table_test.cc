#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"
#include "record/table_iterator.h"
#include "concurrency/transaction_manager.h"
#include "index/hash/static_hash_table.h"
#include "index/hash/hash_table_bucket_page.h"
#include "index/hash/hash_table_directory_page.h"
#include "index/hash/extendible_hash_table.h"


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>
#include <future>


// Macro for time out mechanism
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
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start, Args &&...args) {
  std::vector<std::thread> thread_group;
  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.emplace_back(std::thread(args..., txn_id_start + thread_itr, thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}
// helper function to insert
void InsertHelper(ExtendibleHashTable *hash_table, const std::vector<int> &keys, Transaction *txn, uint64_t tid,  
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    RID value = {key,key};
    hash_table->Insert(Value(key), value, txn);
  }
  EXPECT_NE(keys[0], keys[1]);
}

// helper function to seperate insert
void InsertHelperSplit(ExtendibleHashTable *hash_table, const std::vector<int> &keys,
                       int total_threads, Transaction *txn, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      RID value = {key,key};
          hash_table->Insert(Value(key), value, txn);
    }
  }
}

// helper function to delete
void DeleteHelper(ExtendibleHashTable *hash_table, const std::vector<int> &remove_keys,
               Transaction *txn,   uint64_t tid,  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : remove_keys) {
    RID value = {key,key};
    hash_table->Remove(Value(key), value, txn);
  }
}

// helper function to seperate delete
void DeleteHelperSplit(ExtendibleHashTable *hash_table, const std::vector<int> &remove_keys,
                       int total_threads, Transaction *txn,uint64_t tid,  __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      RID value = {key,key};
          hash_table->Remove(Value(key),value, txn);
    }
  }
}

void LookupHelper(ExtendibleHashTable *hash_table, const std::vector<int> &keys, Transaction *txn,uint64_t tid, 
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    RID value = {key,key};
    std::vector<RID> result;
    bool res = hash_table->GetValue(key, &result, txn);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], value);
  }
}

const size_t NUM_ITERS = 100;

void InsertTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> fm 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> lm 
        = std::make_unique<LogManager>(fm.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(lm.get());

    std::unique_ptr<BufferManager> bfm
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    auto txn_ = txn_mgr.Begin();
    auto txn = txn_.get();

    //
    // make schema
    //
    Schema schema({Column("cola", TypeID::INTEGER)});

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // keys to Insert
    std::vector<int> keys;
    int scale_factor = 100;
    for (int key = 1; key < scale_factor; key++) {
      keys.emplace_back(key);
    }

    LaunchParallelTest(2, 0, InsertHelper, hash.get(), keys, txn);

    std::vector<RID> result;
    for (auto key : keys) {
      result.clear();
      hash->GetValue(key, &result, txn);
      EXPECT_EQ(result.size(), 1);

      RID value = {key, key};
      EXPECT_EQ(result[0], value);
    }

    hash->VerifyHashTable();

  }
}

void InsertTest2Call() {return;
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> fm 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> lm 
        = std::make_unique<LogManager>(fm.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(lm.get());

    std::unique_ptr<BufferManager> bfm
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    auto txn_ = txn_mgr.Begin();
    auto txn = txn_.get();

    //
    // make schema
    //
    Schema schema({Column("cola", TypeID::INTEGER)});

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // keys to Insert
    std::vector<int> keys;
    int scale_factor = 1000;
    for (int key = 1; key < scale_factor; key++) {
      keys.emplace_back(key);
    }

    LaunchParallelTest(2, 0, InsertHelperSplit, hash.get(), keys, 2, txn);

    std::vector<RID> result;
    for (auto key : keys) {
      result.clear();
      hash->GetValue(key, &result, txn);;
      EXPECT_EQ(result.size(), 1);

      RID value = {key,key};
      EXPECT_EQ(result[0], value);
    }

    hash->VerifyHashTable();

  }
}

void DeleteTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> fm 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> lm 
        = std::make_unique<LogManager>(fm.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(lm.get());

    std::unique_ptr<BufferManager> bfm
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    auto txn_ = txn_mgr.Begin();
    auto txn = txn_.get();

    //
    // make schema
    //
    Schema schema({Column("cola", TypeID::INTEGER)});

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // sequential insert
    std::vector<int> keys = {1, 2, 3, 4, 5};
    InsertHelper(hash.get(), keys, txn, 1);

    std::vector<int> remove_keys = {1, 5, 3, 4};
    LaunchParallelTest(2, 1, DeleteHelper, hash.get(), remove_keys, txn);

    int size = 0;
    std::vector<RID> result;
    for (auto key : keys) {
      result.clear();
      RID value = {key,key};
      hash->GetValue(key, &result, txn);;
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }
    EXPECT_EQ(size, keys.size() - remove_keys.size());

    hash->VerifyHashTable();

  }
}

void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> fm 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> lm 
        = std::make_unique<LogManager>(fm.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(lm.get());

    std::unique_ptr<BufferManager> bfm
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    auto txn_ = txn_mgr.Begin();
    auto txn = txn_.get();

    //
    // make schema
    //
    Schema schema({Column("cola", TypeID::INTEGER)});

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // sequential insert
    std::vector<int> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(hash.get(), keys, txn, 1);

    std::vector<int> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, 1, DeleteHelperSplit, hash.get(), remove_keys, 2, txn);

    int size = 0;
    std::vector<RID> result;
    for (auto key : keys) {
      result.clear();
      RID value = {key,key};
      hash->GetValue(key, &result, txn);;
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }
    EXPECT_EQ(size, keys.size() - remove_keys.size());

  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> fm 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> lm 
        = std::make_unique<LogManager>(fm.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(lm.get());

    std::unique_ptr<BufferManager> bfm
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    auto txn_ = txn_mgr.Begin();
    auto txn = txn_.get();

    //
    // make schema
    //
    Schema schema({Column("cola", TypeID::INTEGER)});

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // first, populate index
    std::vector<int> for_insert;
    std::vector<int> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 800;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.emplace_back(i);
      } else {
        for_delete.emplace_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(hash.get(), for_delete, txn, 1);

    auto insert_task = [&](int tid) { InsertHelper(hash.get(), for_insert, txn, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(hash.get(), for_delete, txn, tid); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int size = 0;
    std::vector<RID> result;
    for (auto key : for_insert) {
      result.clear();
      RID value = {key,key};
      hash->GetValue(key, &result, txn);;
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }

    EXPECT_EQ(size, for_insert.size());

    hash->VerifyHashTable();

  }
}

void MixTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> fm 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> lm 
        = std::make_unique<LogManager>(fm.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(lm.get());

    std::unique_ptr<BufferManager> bfm
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    auto txn_ = txn_mgr.Begin();
    auto txn = txn_.get();

    //
    // make schema
    //
    Schema schema({Column("cola", TypeID::INTEGER)});

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // Add perserved_keys
    std::vector<int> perserved_keys;
    std::vector<int> dynamic_keys;
    size_t total_keys = 300;
    size_t sieve = 5;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.emplace_back(i);
      } else {
        dynamic_keys.emplace_back(i);
      }
    }
    InsertHelper(hash.get(), perserved_keys,txn, 1);
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(hash.get(), dynamic_keys, txn, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(hash.get(), dynamic_keys, txn, tid); };
    auto lookup_task = [&](int tid) { LookupHelper(hash.get(), perserved_keys, txn, tid); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    // Check all reserved keys exist
    size = 0;
    std::vector<RID> result;
    for (auto key : perserved_keys) {
      result.clear();
      RID value = {key,key};
      hash->GetValue(key, &result, txn);;
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }

    EXPECT_EQ(size, perserved_keys.size());

    hash->VerifyHashTable();
  }
}

/*
 * Score: 5
 * Description: Concurrently insert a set of keys.
 */
TEST(HashTableConcurrentTest, InsertTest1) {
  TEST_TIMEOUT_BEGIN
  InsertTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: Split the concurrent insert test to multiple threads
 * without overlap.
 */
TEST(HashTableConcurrentTest, InsertTest2) {
  TEST_TIMEOUT_BEGIN
  InsertTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 5
 * Description: Concurrently delete a set of keys.
 */
TEST(HashTableConcurrentTest, DeleteTest1) {
  TEST_TIMEOUT_BEGIN
  DeleteTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: Split the concurrent delete task to multiple threads
 * without overlap.
 */
TEST(HashTableConcurrentTest, DeleteTest2) {
  TEST_TIMEOUT_BEGIN
  DeleteTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(HashTableConcurrentTest2, MixTest1) {
  TEST_TIMEOUT_BEGIN
  MixTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

/*
 * Score: 10
 * Description: Insert a set of keys. Concurrently insert and delete
 * a different set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(HashTableConcurrentTest2, MixTest2) {
  TEST_TIMEOUT_BEGIN
  MixTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

}  // namespace bustub
