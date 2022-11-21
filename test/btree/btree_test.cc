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
    return;;
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
    for (int i = 0;i < 10000;i ++) {
        Tuple tuple({Value(0)}, schema);
        index_key.SetFromKey(tuple);
        btree.Insert(index_key, {i + 1, i + 1});
    }
    

    // read duplicated key
    
    Tuple tuple({Value(0)}, schema);
    index_key.SetFromKey(tuple);
    
    std::vector<RID> res;
    btree.GetValue(index_key, &res);
    ASSERT_EQ(res.size(), 10001);

    for (int i = 0;i < 1001; i++) {
        EXPECT_EQ(res[i], RID(i, i));
    }


    // insert 
}


TEST(BtreeTest, SeqInsertTest) {
    return;;
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


    int key_num = 30000;
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
        btree.Insert(index_key, rid);//btree.PrintTree();
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
    return;;
    // bfm->PrintBufferPool();


    // delete them
    for (uint i = 0; i < keys.size(); i++) {

        auto k = Tuple({Value(keys[i])}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.Remove(index_key, {keys[i], keys[i]}), true);
    }



    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), false);
    }
}




TEST(BtreeTest, BucketRemoveTest) {
    return;;
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


    int value_num = 1000;
    std::vector<RID> values(value_num + 1);
    for (int i = 1; i <= value_num; i++) {
        values[i] = {i,i};
    }

    // insert  keys
    for (int i = 1;i <= value_num; i++) {
        auto value = values[i];
        Tuple tuple({Value(1)}, schema);
        index_key.SetFromKey(tuple);

        (btree.Insert(index_key, value));
    }

    // read 
    {
        std::vector<RID> res;
        btree.GetValue(index_key, &res);
        ASSERT_EQ(res.size(), 1000);
        EXPECT_EQ(res[0], RID(1,1));    
    }


    
    // remove some keys
    for (int i = 2;i <= value_num; i++) {
        Tuple tuple({Value(1)}, schema);
        index_key.SetFromKey(tuple);

        EXPECT_TRUE(btree.Remove(index_key, {i, i}));
    }

    // read 
    {
        std::vector<RID> res;
        btree.GetValue(index_key, &res);
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(1,1));    
    }


}


TEST(BtreeTest, SimpleRemoveTest) {
    return;;
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


    int key_num = 50;
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
    
    // remove a little key
    for (int i = 0;i < 2;i ++) {
        RID value(i, i);
        auto tmp = Tuple({Value(i)}, schema);
        index_key.SetFromKey(tmp);
        btree.Remove(index_key, value);
    }

    // read again
    for (auto key : keys) {
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        if (key >= 2) {
            EXPECT_EQ(btree.GetValue(index_key, &result), true);
            ASSERT_EQ(result.size(), 1);
            EXPECT_EQ(result[0], RID(key, key));
        }
        else {
            EXPECT_EQ(btree.GetValue(index_key, &result), false);
        }
    }

}



TEST(BtreeTest, HigherRemoveTest) {return;

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


    int key_num = 30000;
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




    // remove them
    for (auto key : keys) {
        
            int value = key;
            rid = RID(value, value);
            auto tmp = Tuple({Value(key)}, schema);
            index_key.SetFromKey(tmp);
            btree.Remove(index_key, rid);
    }
    

        // read them
    for (auto key : keys) {
            std::vector<RID> result;
            EXPECT_EQ(btree.GetValue(index_key, &result), false);

        
    } 

    bfm->PrintBufferPool();

}

TEST(BtreeTest, RandomInsertTest) {

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


    int key_num = 30000;
    // std::vector<int> keys {
    //     26, 28, 15, 6, 11, 0, 4, 9, 20, 17, 14, 22, 2, 5, 1, 19, 29, 18
    //     ,3, 10, 25, 7, 23, 16, 13, 24, 27, 12, 21, 8    };

    std::vector<int> keys(key_num);
    for (int i = 0;i < key_num;i ++) {
        keys[i] = i;
    }

    auto rng = std::default_random_engine{};
    std::shuffle(keys.begin(), keys.end(), rng);
    EXPECT_EQ(keys.size(), key_num);
    std::unordered_map<int,int> mp;
    for (auto key:keys) {
        mp[key] ++;
        if (mp[key] > 1) {
            assert(false);
        }
    }

    // insert  keys
    for (int i = 0;i < keys.size();i ++) {
        int key = keys[i];
        // std::cout << "key = " << key << std::endl;
        int value = key;
        rid = RID(value, value);
        auto tmp = Tuple({Value(key)}, schema);
        index_key.SetFromKey(tmp);
        btree.Insert(index_key, rid);
        
        // for (int j = 0;j < i;j ++) {
        //     int key = keys[j];
        //     auto tmp = Tuple({Value(key)}, schema);
        //     index_key.SetFromKey(tmp);
        //     std::vector<RID> result;
        //     btree.GetValue(index_key, &result);
            
        //     if (result.size() == 0) {
        //         btree.PrintTree();
        //     }
        //     ASSERT_EQ(result.size(), 1);
        //     EXPECT_EQ(result[0], RID(key, key));
        // }
        // btree.PrintTree();
    }
    
    // std::cout << "finish\n\n\n" << std::endl;

    // read them
    for (auto key : keys) {
        // std::cout << "key = " << key << std::endl;
        std::vector<RID> result;
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_EQ(btree.GetValue(index_key, &result), true);
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(result[0], RID(key, key));
    } 

    

    // remove them
    for (int i = 0;i < key_num - 100;i ++) {
        int key = keys[i];
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        btree.Remove(index_key, {key, key});
    }
    
     

    for (int i = 0;i < key_num - 100;i ++) {
        std::vector<RID> result;
        int key = keys[i];
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_FALSE(btree.GetValue(index_key, &result));
        EXPECT_EQ(result.size(), 0);
    }

    for (int i = key_num - 100;i < key_num;i ++) {
        std::vector<RID> result;
        int key = keys[i];
        auto k = Tuple({Value(key)}, schema);
        index_key.SetFromKey(k);
        EXPECT_TRUE(btree.GetValue(index_key, &result));
        EXPECT_EQ(result.size(), 1);
    }




    bfm->PrintBufferPool();

}






} // namespace SimpleDB
