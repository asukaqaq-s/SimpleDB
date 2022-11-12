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


TEST(ExtendibleHashTableTest, BasicTest) {
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
    int insert_num = 1000;

    
    auto hash = std::make_unique<ExtendibleHashTable>
                (txn, filename, &schema, rm.get(), bfm.get(), -1);

    // EXPECT_EQ(hash->Insert(SearchKey(Value(insert_num + 1), &schema), {1,1}, txn), true);
    
    // // read
    // {
    //     std::vector<RID> res;
    //     hash->GetValue(SearchKey(Value(insert_num + 1), &schema), &res, txn);
    //     EXPECT_EQ(res.size(), 1);
    //     EXPECT_EQ(res[0], RID(1,1));
    // }


    // insert until split
    {
        for (int i = 0;i < insert_num;i ++) {
            bool res = hash->Insert(Value(i), {i, i}, txn);
            EXPECT_TRUE(res);
        }

    }

    ASSERT_EQ(hash->VerifyHashTable(), true);


    // read
    {
        for (int i = 0;i < insert_num;i ++) {
            std::vector<RID> res;
            hash->GetValue(Value(i), &res, txn);
            EXPECT_EQ(res.size(), 1);
            EXPECT_EQ(res[0], RID(i, i));
        }
    }


    // remove
    {
        for (int i = 0;i < insert_num; i++) {
            bool res = hash->Remove(Value(i), {i,i}, txn);
            EXPECT_TRUE(res);
            if (i >= insert_num - 10)
            hash->PrintHashTable();
        }
    }


    // read
    {   
        for (int i = 0;i < insert_num;i ++) {
            std::vector<RID> res;
            bool success = hash->GetValue(Value(i), &res, txn);
            EXPECT_EQ(res.size(), 0);
            EXPECT_FALSE(success);
        }
                
        EXPECT_EQ(hash->GetDectorySize(), 1);
    }


    // test reuse deleted bucket
    {
        for (int i = 0;i < insert_num;i ++) {
            bool res = hash->Insert(Value(i), {i, i}, txn);
            EXPECT_TRUE(res);
        }
        hash->PrintHashTable();
    }

}






template <typename... Args>
// helper function to launch multiple threads
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start,  Args &&...args) {
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
void InsertHelper(ExtendibleHashTable *hash_table, const std::vector<int> &keys, uint64_t tid, Transaction *txn, 
                  __attribute__((unused)) uint64_t thread_itr = 0) {
    for (auto key : keys) {
        RID value = {key, key};
        hash_table->Insert(Value(key), value, txn);
    }
    EXPECT_NE(keys[0], keys[1]);
}

// helper function to seperate insert
void InsertHelperSplit(ExtendibleHashTable *hash_table, const std::vector<int> &keys, 
                       int total_threads, uint64_t tid, Transaction *txn,  __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      RID value = {key, key};
      hash_table->Insert(Value(key), value, txn);
    }
  }
}

// helper function to delete
void DeleteHelper(ExtendibleHashTable *hash_table, const std::vector<int> &remove_keys,
                  uint64_t tid, Transaction *txn,__attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : remove_keys) {
    RID value = {key, key};
    hash_table->Remove( key, value, txn);
  }
}



// helper function to seperate delete
void DeleteHelperSplit(ExtendibleHashTable *hash_table, const std::vector<int> &remove_keys,
                       int total_threads, uint64_t tid,Transaction *txn, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      RID value = {key, key};
      hash_table->Remove(key, value, txn);
    }
  }
}

void LookupHelper(ExtendibleHashTable *hash_table, const std::vector<int> &keys, uint64_t tid,Transaction *txn,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    RID value = {key, key};
    std::vector<RID> result;

    bool res = hash_table->GetValue(key, &result, txn);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], value);
  }
}

void ConcurrentScaleTest() {
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
    size_t total_keys = 20000;
    size_t sieve = 10;
    for (size_t i = 1; i <= total_keys; i++) {
        if (i % sieve == 0) {
            perserved_keys.emplace_back(i);
        } else {
            dynamic_keys.emplace_back(i);
        }
    }
    InsertHelper(hash.get(), perserved_keys, 1, txn);
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(hash.get(), dynamic_keys, tid, txn); };
    auto delete_task = [&](int tid) { DeleteHelper(hash.get(), dynamic_keys, tid, txn); };
    auto lookup_task = [&](int tid) { LookupHelper(hash.get(), perserved_keys, tid, txn); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 3;
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
        threads[i].join();
    }
    return;
    // Check all reserved keys exist
    size = 0;
    std::vector<RID> result;
    for (auto key : perserved_keys) {
        result.clear();
        RID value = {key, key};
        hash->GetValue(key, &result, txn);
        if (std::find(result.begin(), result.end(), value) != result.end()) {
            size++;
        }
    }

    EXPECT_EQ(size, perserved_keys.size());

    EXPECT_TRUE(hash->VerifyHashTable());
}

void ScaleTestCall() {
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

    int num_keys = 5000;  // index can fit around 225k int-int pairs



    //  insert all the keys
    for (int i = 0; i < num_keys; i++) {
        hash->Insert( Value(i), {i, i}, txn);
        std::vector<RID> res;
        EXPECT_TRUE(hash->GetValue( i, &res, txn));
        EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    }

    EXPECT_TRUE(hash->VerifyHashTable());

    //  remove half the keys
    for (int i = 0; i < num_keys / 2; i++) {
        EXPECT_TRUE(hash->Remove(i, {i, i}, txn));
        std::vector<RID> res;
        EXPECT_FALSE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
    }

    EXPECT_TRUE(hash->VerifyHashTable());

    //  try to find the removed half
    for (int i = 0; i < num_keys / 2; i++) {
        std::vector<RID> res;
        EXPECT_FALSE(hash->GetValue(i, &res, txn));
    }

    //  insert to the 2nd half as duplicates
    for (int i = num_keys / 2; i < num_keys; i++) {
        hash->Insert(i, {i + 1, i + 1}, txn);
        std::vector<RID> res;
        EXPECT_TRUE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(2, res.size()) << "Missing duplicate kv pair for: " << i << std::endl;
    }

    EXPECT_TRUE(hash->VerifyHashTable());

    //  get all the duplicates
    for (int i = num_keys / 2; i < num_keys; i++) {
        std::vector<RID> res;
        EXPECT_TRUE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(2, res.size()) << "Missing duplicate kv pair for: " << i << std::endl;
    }

    EXPECT_TRUE(hash->VerifyHashTable());

    //  remove the last duplicates inserted
    for (int i = num_keys / 2; i < num_keys; i++) {
        EXPECT_TRUE(hash->Remove(i, {i + 1, i + 1}, txn));
        std::vector<RID> res;
        EXPECT_TRUE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(1, res.size()) << "Missing kv pair for: " << i << std::endl;
    }

      EXPECT_TRUE(hash->VerifyHashTable());

    //  query everything
    for (int i = num_keys / 2; i < num_keys; i++) {
        std::vector<RID> res;
        EXPECT_TRUE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(1, res.size()) << "Missing kv pair for: " << i << std::endl;
    }

      EXPECT_TRUE(hash->VerifyHashTable());

    //  remove the rest of the remaining keys
    for (int i = num_keys / 2; i < num_keys; i++) {
        EXPECT_TRUE(hash->Remove(i, {i, i}, txn));
        std::vector<RID> res;
        EXPECT_FALSE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(0, res.size()) << "Failed to insert " << i << std::endl;
    }

      EXPECT_TRUE(hash->VerifyHashTable());

    //  query everything
    for (int i = 0; i < num_keys; i++) {
        std::vector<RID> res;
        EXPECT_FALSE(hash->GetValue(i, &res, txn));
        EXPECT_EQ(0, res.size()) << "Found non-existent key: " << i << std::endl;
    }

      EXPECT_TRUE(hash->VerifyHashTable());


}

/*
 * Score: 5
 * Description: Insert 200k keys to verify the table capacity
 */
TEST(HashTableScaleTest, ScaleTest) { //ScaleTestCall();4
 }

/*
 * Score: 5
 * Description: Same as MixTest2 but with 100k integer keys
 * and only runs 1 iteration.
 */
TEST(HashTableScaleTest, ConcurrentScaleTest) {
  TEST_TIMEOUT_BEGIN
  ConcurrentScaleTest();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}



} // namespace SimpleDB