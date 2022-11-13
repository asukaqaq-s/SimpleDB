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
#include "index/hash/test_bucket.h"


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>



namespace SimpleDB {

TEST(TestBucketTest, BasicTest) {
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
    auto txn = txn_mgr.Begin();
    int n;
    auto bucket = reinterpret_cast<TestBucket*>
                (bfm->NewBlock(filename, &n)->contents()->GetRawDataPtr());
    
    bucket->data_[0] = {Value(1), {1, 1}};
    bucket->data_[1] = {Value(2), {2, 2}};
    bucket->data_[2] = {Value(3), {3, 3}};

    bfm->UnpinBlock(BlockId(filename, n), true);
    
    bucket = reinterpret_cast<TestBucket*>(bfm->PinBlock({filename, n})->contents()->GetRawDataPtr());
    

    for (int i = 0;i < 3; i++) {
        std::cout << bucket->data_[i].first.to_string() << " " 
                  << bucket->data_[i].second.ToString() << std::endl; 
    }

}



} // namespace SimpleDB