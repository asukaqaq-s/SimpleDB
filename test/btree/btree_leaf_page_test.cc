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


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>
#include <future>


namespace SimpleDB {

TEST(BtreeLeafPageTest, BasicTest) {
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
    // auto txn_ = txn_mgr.Begin();
    // auto txn = txn_.get();
    int n;
    auto leaf1 = static_cast<BPlusTreeLeafPage*> (bfm->NewBlock(test_file, &n));
    auto leaf2 = static_cast<BPlusTreeLeafPage*> (bfm->NewBlock(test_file, &n));
    leaf1->Init(0, 4, TypeID::INTEGER);
    leaf2->Init(0, 4, TypeID::INTEGER);
    

    // leaf1 insert
    for (int i = 0;i < 10;i ++) {
        leaf1->Insert(Value(i), {i, i});
    }

    // leaf1 read
    for (int i = 0;i < 10;i ++) {
        std::vector<RID> res;
        int n;
        EXPECT_TRUE(leaf1->GetValue(Value(i), &res, &n));
        EXPECT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(i,i));
    }

    // leaf1 remove
    for (int i = 0;i < 10;i ++) {
        if (i & 1) continue;
        leaf1->Remove(Value(i), {i, i});
    }

    // leaf1 read again
    for (int i = 0;i < 10;i ++) {
        if (!(i & 1)) continue;
        std::vector<RID> res;
        int n;
        EXPECT_TRUE(leaf1->GetValue(Value(i), &res, &n));
        EXPECT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(i,i));
    }

    // leaf1 insert deleted record
    for (int i = 0;i < 10;i ++) {
        if (i & 1) continue;
        leaf1->Insert(Value(i), {i, i});
    }

    // leaf1 get data
    for (int i = 0;i < 10;i ++) {
        std::vector<RID> res;
        int n;
        EXPECT_TRUE(leaf1->GetValue(Value(i), &res, &n));
        EXPECT_EQ(res.size(), 1);
        EXPECT_EQ(res[0], RID(i,i));
    }


    // leaf2 insert some key
    for (int i = 10;i < 20;i ++) {
        leaf2->Insert(Value(i), {i, i});
    } 

    // move first val to leaf1
    leaf2->MoveFirstToEndOf(leaf1);
    leaf1->PrintLeaf();
    leaf2->PrintLeaf();

    // move half data to leaf1
    leaf2->MoveHalfTo(leaf1);
    leaf1->PrintLeaf();
    leaf2->PrintLeaf();
    
    // move end val to leaf2
    leaf1->MoveLastToFrontOf(leaf2);


    leaf1->PrintLeaf();
    leaf2->PrintLeaf();
    

}

} // namespace SimpleDB