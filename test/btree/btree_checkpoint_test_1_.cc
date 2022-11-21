#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"
#include "record/table_iterator.h"
#include "concurrency/transaction_manager.h"
#include "index/btree/b_plus_tree_leaf_page.h"
#include "index/btree/b_plus_tree_directory_page.h"
#include "index/btree/b_plus_tree.h"


#include <time.h>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>
#include <future>

#define DEBUG

namespace SimpleDB {

TEST(BPlusTreeTest, SequentialInsertTest) {
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
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 50);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    int key_num = 10000;
    RID rid;
    std::vector<int> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }

    clock_t begin,end;
    begin = clock();
    // insert  keys
    for (auto key : keys) {
        int value = key;
        rid = RID(value, value);
        auto tmp = Tuple({Value(key)}, schema);
        index_key.SetFromKey(tmp);
        btree.Insert(index_key, rid);
    } 
            
    end = clock();
    std::cout << "insert time  =  " << double(end - begin) << std::endl;
    *btree.GetFindTime() = 0;
    // EXPECT_EQ(bfm->CheckPinCount(), true);

    begin = clock();

    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), true);
        EXPECT_EQ(result[0], RID(key, key));
    }
    
    end = clock();
    std::cout << "read time  =  " << double(end - begin) << std::endl;
    std::cout << "read find time = " << *btree.GetFindTime() << std::endl;
    std::cout << "read read time = " << btree.GetReadTime() << std::endl; 
    
    begin = clock();
    // // return;
    //EXPECT_EQ(bfm->CheckPinCount(), true);// return;
    // delete them
    for (uint i = 0; i < keys.size(); i++) {
        RID value(i,i);
 
        auto k = Tuple({Value(keys[i])}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.Remove(index_key, value), true);
        //if (i == 168) {
            // break;
        //}
    }
    end = clock();
    std::cout << "remove time  =  " << double(end - begin) << std::endl;

    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), false);
    }
    // 
    
    EXPECT_EQ(bfm->CheckPinCount(), true);
    bfm->PrintBufferPool();
    remove(filename.c_str());

}

TEST(BPlusTreeTest, RandomInsertTest) {// return;
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


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    RID rid;

    int key_num = 10000;
    std::vector<int> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());


    // insert  keys
    for (auto key : keys) {
        int value = key;
        rid = RID(value, value);
        auto tmp = Tuple({Value(key)}, schema);
        index_key.SetFromKey(tmp);
        btree.Insert(index_key, rid);
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), true);
        EXPECT_EQ(result[0], RID(key, key));
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // delete them
    for (uint i = 0; i < keys.size(); i++) {
        auto k = Tuple({Value(keys[i])}, schema);
        index_key.SetFromKey(k);
        RID value(keys[i], keys[i]);
        EXPECT_EQ(btree.Remove(index_key, value), true);
        
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), false);
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    
}

TEST(BPlusTreeTest, ConcurrentBasicTest) {// return;
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


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    RID rid;

    int key_num = 1000;
    int worker_num = 5;
    std::vector<int> keys(key_num * worker_num);
    for (int i = 0; i < key_num * worker_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    std::vector<std::thread> worker_list;

    EXPECT_EQ(bfm->CheckPinCount(), true);

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            GenericKey<4> index_key;
            for (int j = begin; j < end; j++) {
        
                auto rid = RID(keys[j], keys[j]);
                auto tmp = Tuple({Value(keys[j])}, schema);
                index_key.SetFromKey(tmp);
                btree.Insert(index_key, rid);

                // could we read what we just write?
                std::vector<RID> result;
                EXPECT_EQ(btree.GetValue(index_key, &result), true);
                EXPECT_EQ(result[0], RID(keys[j], keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);
    
    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        GenericKey<4> index_key;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), true);
        EXPECT_EQ(result[0], RID(key, key));
    }
    
    EXPECT_EQ(bfm->CheckPinCount(), true);

    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            GenericKey<4> index_key;
            for (int j = begin; j < end; j++) {
                RID value(keys[j], keys[j]);
                auto k = Tuple({Value(keys[j])}, schema);
                index_key.SetFromKey(k);
                EXPECT_EQ(btree.Remove(index_key, value), true);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    for (auto key : keys) {
        GenericKey<4> index_key;
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), false);
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);


    remove(filename.c_str());
}

TEST(BPlusTreeTest, ConcurrentStrictTest) {// return;
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
        = std::make_unique<BufferManager>(fm.get(), rm.get(), 50);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    RID rid;

    int key_num = 2000;
    int worker_num = 8;
    std::vector<int> keys(key_num * worker_num);
    for (int i = 0; i < key_num * worker_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    std::vector<std::thread> worker_list;
    std::vector<std::thread> daemon_list;

    std::atomic_bool shutdown(false);
    // there will be a worker keep on reading
    daemon_list.emplace_back(std::thread([&](){
        
        GenericKey<4> index_key;
        std::random_device rd;
        std::mt19937 mt(rd());
        std::uniform_int_distribution<int> dis(0, key_num * worker_num - 1);
        while (shutdown.load() == false) {
    
            int idx = dis(mt);
            std::vector<RID> result;
            auto tmp = Tuple({Value(keys[idx])}, schema);
            index_key.SetFromKey(tmp);
            btree.GetValue(index_key, &result);
        }
    }));

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            
            GenericKey<4> index_key;
            for (int j = begin; j < end; j++) {
        
                auto rid = RID(keys[j], keys[j]);
                auto tmp = Tuple({Value(keys[j])}, schema);
                index_key.SetFromKey(tmp);
                btree.Insert(index_key, rid);

                // could we read what we just write?
                std::vector<RID> result;
                EXPECT_EQ(btree.GetValue(index_key, &result), true);
                EXPECT_EQ(result[0], RID(keys[j], keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }
    
    // read them
    for (auto key : keys) {
        
        std::vector<RID> result;
        GenericKey<4> index_key;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), true);
        EXPECT_EQ(result[0], RID(key, key));
    }

    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            
            GenericKey<4> index_key;
            for (int j = begin; j < end; j++) {
                
                RID value(keys[j], keys[j]);
                auto k = Tuple({Value(keys[j])}, schema);
                index_key.SetFromKey(k);
                EXPECT_EQ(btree.Remove(index_key, value), true);

                // we shouldn't see what we just deleted
                std::vector<RID> result;
                EXPECT_EQ(btree.GetValue(index_key, &result), false);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    for (auto key : keys) {
        
        GenericKey<4> index_key;
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), false);
    }

    shutdown.store(true);
    for (uint i = 0; i < daemon_list.size(); i++) {
        daemon_list[i].join();
    }


}

TEST(BPlusTreeTest, BasicIteratorTest) {
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


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    RID rid;


    int key_num = 10000;
    std::vector<int> keys(key_num);
    std::vector<RID> keys_sorted(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
        keys_sorted[i] = {i,i};
    }
    std::random_shuffle(keys.begin(), keys.end());

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // insert  keys
    for (auto key : keys) {

        int value = key;
        rid = RID(value, value);
        auto tmp = Tuple({Value(key)}, schema);
        index_key.SetFromKey(tmp);
        btree.Insert(index_key, rid);
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // read them by iterator
    int ptr = 0;
    for (auto it = btree.Begin(); !it->IsEnd(); it->Advance()) {
        std::vector<RID> res;
        it->GetValue(&res);
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, key_num);

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // delete them
    for (uint i = 0; i < keys.size(); i++) {

        RID value(keys[i], keys[i]);
        auto k = Tuple({Value(keys[i])}, schema);
        index_key.SetFromKey(k);

        EXPECT_EQ(btree.Remove(index_key, value), true);
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // read them by iterator
    ptr = 0;

    for (auto it = btree.Begin(); !it->IsEnd(); it->Advance()) {
        std::vector<RID> res;
        it->GetValue(&res);
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(keys_sorted[ptr]));
        ptr++;
    }

    EXPECT_EQ(ptr, 0);
    EXPECT_EQ(bfm->CheckPinCount(), true);
}


TEST(BPlusTreeTest, BasicIteratorTest2) {
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


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    RID rid;


    int key_num = 10000;
    std::vector<int> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // insert  keys
    for (auto key : keys) {

        int value = key;
        rid = RID(value, value);
        auto tmp = Tuple({Value(key)}, schema);
        index_key.SetFromKey(tmp);
        btree.Insert(index_key, rid);
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);

    // read them by iterator
    for (int i = 0;i < 10000;i ++) {
        index_key.SetFromInteger(keys[i]);
        auto it = btree.Begin(index_key);
        std::vector<RID> res;
        it->GetValue(&res);
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(keys[i], keys[i]));
    }
    
        
    

    EXPECT_EQ(bfm->CheckPinCount(), true);
}



TEST(BPlusTreeTest, ConcurrentIteratorTest) {
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


    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    
    GenericComparator<4> comparator(&schema);
    BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
                                                             comparator, bfm.get());
    GenericKey<4> index_key;

    RID rid;

    int key_num = 2000;
    int worker_num = 8;
    std::vector<int> keys(key_num * worker_num);
    std::vector<int> keys_sorted(key_num * worker_num);
    for (int i = 0; i < key_num * worker_num; i++) {
        keys[i] = i;
        keys_sorted[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    std::vector<std::thread> worker_list;
    std::vector<std::thread> daemon_list;

    std::atomic_bool shutdown(false);


    // there will be a worker keep on reading
    // daemon_list.emplace_back(std::thread([&](){
    //     while (shutdown.load() == false) {
    //         // INVARIANT: the result should always be an ascending sequence
    //         int last = -1;
    //         int read_cnt = 0;
    //         auto it = btree.Begin();
    //         for (; !it->IsEnd(); it->Advance()) {
    //             int cur = it->GetKey().AsInt();
    //             EXPECT_GT(cur, last);
    //             last = cur;
    //             read_cnt++;
    //         }
    //         // LOG_INFO("retry times %d read_cnt %d", it.GetRetryCnt(), read_cnt);
    //     }
    // }));

    

    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            
            GenericKey<4> index_key;
            for (int j = begin; j < end; j++) {
        
                auto rid = RID(keys[j], keys[j]);
                auto tmp = Tuple({Value(keys[j])}, schema);
                index_key.SetFromKey(tmp);
                btree.Insert(index_key, rid);

                // could we read what we just write?
                std::vector<RID> result;
                EXPECT_EQ(btree.GetValue(index_key, &result), true);
                EXPECT_EQ(result[0], RID(keys[j], keys[j]));
            }
        }, i * key_num, (i + 1) * key_num));
    }


    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }


    // read them by iterator
    int ptr = 0;
    for (auto it = btree.Begin(); !it->IsEnd(); it->Advance()) {
        EXPECT_EQ(it->GetKey().AsInt(), keys_sorted[ptr]);
        ptr++;
    }
    EXPECT_EQ(ptr, key_num * worker_num);


// return;
    // delete them
    worker_list.clear();
    for (int i = 0; i < worker_num; i++) {
        worker_list.emplace_back(std::thread([&](int begin, int end) {
            
            GenericKey<4> index_key;
            for (int j = begin; j < end; j++) {
        
                auto k = Tuple({Value(keys[j])}, schema);
                auto value = RID(keys[j], keys[j]);
                index_key.SetFromKey(k);
                EXPECT_EQ(btree.Remove(index_key, value), true);

                // we shouldn't see what we just deleted
                std::vector<RID> result;
                EXPECT_EQ(btree.GetValue(index_key, &result), false);
            }
        }, i * key_num, (i + 1) * key_num));
    }
    for (uint i = 0; i < worker_list.size(); i++) {
        worker_list[i].join();
    }

    ptr = 0;
    for (auto it = btree.Begin(); !it->IsEnd(); it->Advance()) {
        EXPECT_EQ(it->GetKey().AsInt(), (keys_sorted[ptr]));
        ptr++;
    }
    EXPECT_EQ(ptr, 0);

    shutdown.store(true);
    for (uint i = 0; i < daemon_list.size(); i++) {
        daemon_list[i].join();
    }

    EXPECT_EQ(bfm->CheckPinCount(), true);


    remove(filename.c_str());
}

}