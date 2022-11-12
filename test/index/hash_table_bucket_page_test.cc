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


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>



namespace SimpleDB {

TEST(HashTableBucketPageTest, BasicTest) {
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
    auto *bucket = static_cast<HashTableBucketPage*> (bfm->NewBlock(filename, &n));

    // init this bucket
    bucket->InitHashBucketPage(4 + 8, TypeID::INTEGER);

    // insert
    for (int i = 0;i < 10;i ++) {
        EXPECT_TRUE(bucket->Insert(Value(i), {i,i}));
    }

    bucket->PrintBucket();

    // read
    for (int i = 0;i < 10;i ++) {
        std::vector<RID> res;
        bucket->GetValue(Value(i), &res);
        EXPECT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(i, i));
    }


    // remove
    for (int i = 0;i < 10;i ++) {
        if (i % 2) {
            bucket->Remove(Value(i), {i,i});
        }
        else {
            continue;
        }
    }

    // read again
    for (int i = 0;i < 10;i ++) {
        if (i % 2 == 0) {
            std::vector<RID> res;
            bucket->GetValue(Value(i), &res);
            EXPECT_EQ(res.size(), 1);
            EXPECT_EQ(res[0], RID(i, i));
        }
    }
    
}

TEST(HashTableBucketPageTest, InterTest) {
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
    auto *bucket = static_cast<HashTableBucketPage*> (bfm->NewBlock(filename, &n));

    // init this bucket
    bucket->InitHashBucketPage(4 + 8, TypeID::INTEGER);

    // insert
    int count = 0;
    for (int i = 0;i < 1000;i ++) {
        if (!bucket->Insert(Value(i), {i,i})) {
            count ++;
            break;
        }
    }

    bucket->PrintBucket();

    // read
    for (int i = 0;i < count;i ++) {
        std::vector<RID> res;
        bucket->GetValue(Value(i), &res);
        EXPECT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(i, i));
    }


    // remove
    for (int i = 0;i < count;i ++) {
        if (i % 2) {
            bucket->Remove(Value(i), {i,i});
        }
        else {
            continue;
        }
    }

    // read again
    for (int i = 0;i < count;i ++) {
        if (i % 2 == 0) {
            std::vector<RID> res;
            bucket->GetValue(Value(i), &res);
            EXPECT_EQ(res.size(), 1);
            EXPECT_EQ(res[0], RID(i, i));
        }
    }
    
}

TEST(HashTableBucketPageTest, StringTest) {
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
    auto *bucket = static_cast<HashTableBucketPage*> (bfm->NewBlock(filename, &n));
    std::string str1(10, '1');
    std::string str2(10, '2');
    // init this bucket
    bucket->InitHashBucketPage(14 + 8, TypeID::VARCHAR);

    // insert
    int count1 = 0;
    int count2 = 0;
    for (int i = 0;i < 1000;i ++) {
        if (i % 2 == 0) {
            count1 ++;
            if (!bucket->Insert(Value(str1, TypeID::VARCHAR), {i,i})) {
                count1--;
                break;
            }
        }
        else {
            count2 ++;
            if (!bucket->Insert(Value(str2, TypeID::VARCHAR), {i,i})) {
                count2--;    
                break;
            }
        }
    }
    // std::cout << count1 << "  " << count2 << std::endl;
    bucket->PrintBucket();

    // // read
    // for (int i = 0;i < count;i ++) {
    //     if (i % 2 == 0) {
            std::vector<RID> res0;
            bucket->GetValue(Value(str1, TypeID::VARCHAR), &res0);
            EXPECT_EQ(res0.size(), count1);
            for (int i = 0;i < count1;i ++)
                EXPECT_EQ(res0[i], RID(i * 2, i * 2));
    //     }
    //     else {
            std::vector<RID> res1;
            bucket->GetValue(Value(str2, TypeID::VARCHAR), &res1);
            EXPECT_EQ(res1.size(), count2);
            for (int i = 0;i < count2;i++)
                EXPECT_EQ(res1[i], RID(i * 2+ 1, i * 2 + 1));
    //     }
    // }


    // // remove
    // for (int i = 0;i < count;i ++) {
    //     if (i % 2 == 0) {
        for (int i = 0; i < count1;i++)
           bucket->Remove(Value(str1, TypeID::VARCHAR), {i * 2,i * 2});
    //     }
    //     else {
    //         continue;
    //     }
    // }

    std::vector<RID> res2;
            bucket->GetValue(Value(str1, TypeID::VARCHAR), &res2);
            EXPECT_EQ(res2.size(), 0);
    //     }
    //     else {
            std::vector<RID> res3;
            bucket->GetValue(Value(str2, TypeID::VARCHAR), &res3);
            EXPECT_EQ(res3.size(), count2);
            for (int i = 0;i < count2;i++)
                EXPECT_EQ(res3[i], RID(i * 2 + 1, i * 2 + 1));
    
    
}



} // namespace SimpleDB