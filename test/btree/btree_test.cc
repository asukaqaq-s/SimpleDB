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


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>
#include <future>


namespace SimpleDB {


TEST(BtreeTest, BasicTest) {
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
    
    for (int i = 0;i < 10;i ++) {
        Tuple tuple({Value(i)}, schema);
        index_key.SetFromKey(tuple);
        btree.Insert(index_key, {i, i});
    }

    

    for (int i = 0;i < 10;i ++) {
        Tuple tuple({Value(i)}, schema);
        index_key.SetFromKey(tuple);
    
        std::vector<RID> res;
        btree.GetValue(index_key, &res);
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(i, i));
    }

    // insert duplicated key
    for (int i = 0;i < 10;i ++) {
        Tuple tuple({Value(i)}, schema);
        index_key.SetFromKey(tuple);
        btree.Insert(index_key, {i + 1, i + 1});
    }
    

    // read duplicated key
    for (int i = 0;i < 10;i ++) {
        Tuple tuple({Value(i)}, schema);
        index_key.SetFromKey(tuple);
    
        std::vector<RID> res;
        btree.GetValue(index_key, &res);
        ASSERT_EQ(res.size(), 2);
        EXPECT_EQ(res[0], RID(i, i));
        EXPECT_EQ(res[1], RID(i + 1, i + 1));
    }


    // insert 
}


TEST(BtreeTest, SeqInsertTest) {
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


    int key_num = 100000;
    std::vector<int> keys(key_num);
    for (int i = 0; i < key_num; i++) {
        keys[i] = i;
    }
    // insert  keys
    for (auto key : keys) {
        int value = key;
        rid = RID(value, value);
        auto tmp = Tuple({Value(key)}, schema);
        index_key.SetFromKey(tmp);
        btree.Insert(index_key, rid);
    }

    // read them
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), true);
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(result[0], RID(key, key));
    }

    // bfm->PrintBufferPool();


    // // delete them
    // for (uint i = 0; i < keys.size(); i++) {
    //     context.Reset();
    //     auto k = Tuple({Value(TypeId::BIGINT, keys[i])}, &schema);
    //     index_key.SetFromKey(k);
    //     EXPECT_EQ(btree.Remove(index_key, &context), true);
    // }

    // for (auto key : keys) {
    //     std::vector<RID> result;
    //     auto k = Tuple({Value(TypeId::BIGINT, key)}, &schema);
    //     index_key.SetFromKey(k);
    //     EXPECT_EQ(btree.GetValue(index_key, &result, &context), false);
    // }
}

} // namespace SimpleDB
