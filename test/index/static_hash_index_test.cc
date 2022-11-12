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


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>



namespace SimpleDB {

TEST(StaticHashTest, BasicTest) {
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
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
    auto txn = txn_mgr.Begin();

    // make kv schema
    std::vector<Column> vec 
    {
        Column("key", TypeID::INTEGER),
        Column("block", TypeID::INTEGER),
        Column("slot", TypeID::INTEGER)
    };
    Schema schema(vec);
    Schema key_schema({Column("key", TypeID::INTEGER)});

    // make hash table
    auto static_hash = std::make_unique<StaticHashTable>("hash1", &schema, rm.get());

    for (int i = 0;i < 1000;i ++) {
        static_hash->Insert(txn.get(), Value(i), RID(i,2*i));
    }

    for (int i = 0;i < 1000;i ++) {
        std::vector<RID> result;
        static_hash->Read(txn.get(), Value(i), &result);
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0], RID(i, 2*i));
    }

    for (int i = 0;i < 1000;i ++) {
        if (i % 2) continue;
        EXPECT_TRUE(static_hash->Remove(txn.get(), Value(i), RID(i, 2*i)));
    }

    for (int i = 0;i < 1000;i ++) {
        std::vector<RID> result;
        if (i % 2) {         
            static_hash->Read(txn.get(), Value(i), &result);
            EXPECT_EQ(result.size(), 1);
            EXPECT_EQ(result[0], RID(i, 2*i));
        }
        else {
            EXPECT_FALSE(static_hash->Read(txn.get(), Value(i), &result));
        }
    }


}


TEST(StaticHashTest, DecimalTest) {
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
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
    auto txn = txn_mgr.Begin();

    // make kv schema
    std::vector<Column> vec 
    {
        Column("key", TypeID::DECIMAL),
        Column("block", TypeID::INTEGER),
        Column("slot", TypeID::INTEGER)
    };
    Schema schema(vec);
    Schema key_schema({Column("key", TypeID::DECIMAL)});

    // make hash table
    auto static_hash = std::make_unique<StaticHashTable>("hash1", &schema, rm.get());

    for (int i = 0;i < 1000;i ++) {
        static_hash->Insert(txn.get(), Value(double(i)), RID(i,2*i));
    }

    for (int i = 0;i < 1000;i ++) {
        std::vector<RID> result;
        static_hash->Read(txn.get(), Value(double(i)), &result);
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0], RID(i, 2*i));
    }

    for (int i = 0;i < 1000;i ++) {
        if (i % 2) continue;
        EXPECT_TRUE(static_hash->Remove(txn.get(), Value(double(i)), RID(i, 2*i)));
    }

    for (int i = 0;i < 1000;i ++) {
        std::vector<RID> result;
        if (i % 2) {         
            static_hash->Read(txn.get(), Value(double(i)), &result);
            EXPECT_EQ(result.size(), 1);
            EXPECT_EQ(result[0], RID(i, 2*i));
        }
        else {
            EXPECT_FALSE(static_hash->Read(txn.get(), Value(double(i)), &result));
        }
    }


}





std::string GenerateKey(int size) {
    std::string str;
    for (int i = 0;i < size;i ++) {
        str += '0' + (i % 10);
    }
    return str;
}



TEST(StaticHashTest, StringTest) {
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
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
    auto txn = txn_mgr.Begin();

    // make kv schema
    std::vector<Column> vec 
    {
        Column("key", TypeID::VARCHAR, 10),
        Column("block", TypeID::INTEGER),
        Column("slot", TypeID::INTEGER)
    };
    Schema schema(vec);
    Schema key_schema({Column("key", TypeID::VARCHAR, 10)});

    // make hash table
    auto static_hash = std::make_unique<StaticHashTable>("hash1", &schema, rm.get());


    // insert
    for (int i = 0;i < 1000;i ++) {
        if (i % 2) {
            auto value = Value(GenerateKey(20), TypeID::VARCHAR);
            static_hash->Insert(txn.get(), value, RID(i,2*i));
        }
        else {
            auto value = Value(GenerateKey(5), TypeID::VARCHAR);
            static_hash->Insert(txn.get(), value, RID(i,2*i));
        }
    }


    // read
    std::vector<RID> result1;
    std::vector<RID> result2;
    auto value = Value(GenerateKey(20), TypeID::VARCHAR);
    static_hash->Read(txn.get(), value, &result1);
    EXPECT_EQ(result1.size(), 500);
    value = Value(GenerateKey(5), TypeID::VARCHAR);
    static_hash->Read(txn.get(), value, &result2);
    EXPECT_EQ(result2.size(), 500);

    for (int i = 0;i < 1000;i ++) {
        if (i % 2) {
            EXPECT_EQ(result1[i >> 1], RID(i, 2*i));
        }
        else {
            EXPECT_EQ(result2[i >> 1], RID(i, 2*i));
        }
    }


    // remove
    for (int i = 0;i < 1000;i ++) {
        if (i % 2) {
            auto value = Value(GenerateKey(20), TypeID::VARCHAR);
            EXPECT_TRUE(static_hash->Remove(txn.get(), value, RID(i, 2*i)));
        }
    }


    // read again
    for (int i = 0;i < 1000;i ++) {
        std::vector<RID> result;
        if (i % 2) {         
            auto value = Value(GenerateKey(20), TypeID::VARCHAR);
            static_hash->Read(txn.get(), value, &result);
            EXPECT_EQ(result.size(), 0);
        }
        else {
            std::vector<RID> result2;
            value = Value(GenerateKey(5), TypeID::VARCHAR);
            static_hash->Read(txn.get(),value, &result2);
            EXPECT_EQ(result2.size(), 500);
        }
    }


}



} // namespace SimpleDB