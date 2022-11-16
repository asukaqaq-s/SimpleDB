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
#include "index/btree/b_plus_tree_bucket_page.h"


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>
#include <future>


namespace SimpleDB {



TEST(BtreeLeafPageTest, BasicTest) {
    // const std::string filename = "test.db";
    // char buf[100];
    // std::string local_path = getcwd(buf, 100);
    // std::string test_dir = local_path + "/" + "test_dir";
    // std::string test_file = "test1.txt";
    // std::string cmd;
    // cmd = "rm -rf " + test_dir;
    // system(cmd.c_str());

    // std::string log_file_name = "log.log";
    // std::string log_file_path = test_dir + "/" + log_file_name;
    // std::unique_ptr<FileManager> fm 
    //     = std::make_unique<FileManager>(test_dir, 4096);
    
    // std::unique_ptr<LogManager> lm 
    //     = std::make_unique<LogManager>(fm.get(), log_file_name);

    // std::unique_ptr<RecoveryManager> rm 
    //     = std::make_unique<RecoveryManager>(lm.get());

    // std::unique_ptr<BufferManager> bfm
    //     = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

    // // --------------------
    // //  create execution context
    // // --------------------
    // auto lock = std::make_unique<LockManager> ();
    // TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());
    // // auto txn_ = txn_mgr.Begin();
    // // auto txn = txn_.get();
    // auto colA = Column("colA", TypeID::INTEGER);
    // std::vector<Column> cols;
    // cols.push_back(colA);
    // auto schema = Schema(cols);

    // GenericComparator<4> comparator(&schema);
    // GenericKey<4> index_key;

   
    // auto *leaf1 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<4>, int, GenericComparator<4>> *>
    //             (bfm->NewBlock(filename, nullptr)->contents()->GetRawDataPtr());

    // leaf1->Init(0);
    // for (int i = 10;i >= 1;i --) {
    //     Tuple tuple({Value(i)}, schema);
    //     index_key.SetFromKey(tuple);
    //     leaf1->Insert(index_key, i, comparator);
    // }

    // leaf1->PrintLeaf();

    // for (int i = 1;i <= 10;i ++) {
    //     if (i % 2) continue;
    //     Tuple tuple({Value(i)}, schema);
    //     index_key.SetFromKey(tuple);
    //     leaf1->Remove(index_key, comparator);
    // }

    // leaf1->PrintLeaf();



    // auto *leaf2 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<4>, int, GenericComparator<4>> *>
    //             (bfm->NewBlock(filename, nullptr)->contents()->GetRawDataPtr());
    // leaf2->Init(1);

    // for (int i = 11;i <= 20;i ++) {
    //     Tuple tuple({Value(i)}, schema);
    //     index_key.SetFromKey(tuple);
    //     leaf2->Insert(index_key, i, comparator);
    // }

    // leaf2->MoveHalfTo(leaf1);
    // leaf1->PrintLeaf();
    // leaf2->PrintLeaf();

    // leaf1->MoveFirstToEndOf(leaf2);
    // leaf1->PrintLeaf();
    // leaf2->PrintLeaf();

    // leaf1->MoveLastToFrontOf(leaf2);
    // leaf1->PrintLeaf();
    // leaf2->PrintLeaf();

}


TEST(BtreeDirectoryPageTest, BasicTest) {
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
    auto colA = Column("colA", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    auto schema = Schema(cols);

    GenericComparator<4> comparator(&schema);
    GenericKey<4> index_key;

    auto *dir1 = reinterpret_cast<BPlusTreeDirectoryPage<GenericKey<4>, int, GenericComparator<4>> *>
                (bfm->NewBlock(filename, nullptr)->contents()->GetRawDataPtr());

    dir1->Init(1);
    Tuple tuple({Value(10)}, schema);
    index_key.SetFromKey(tuple);
    dir1->InitNewRoot(index_key, 10, 11);

    for (int i = 1;i < 10;i ++) {
        Tuple tuple({Value(i)}, schema);
        index_key.SetFromKey(tuple);
        dir1->InsertNode(index_key, i, comparator);
    }

    dir1->PrintDir();

    for (int i = 1;i <= 10;i ++) {
        Tuple tuple({Value(i)}, schema);
        index_key.SetFromKey(tuple);
        std::cout << dir1->Lookup(index_key, comparator) << "   ";
    }


    return;
}


} // namespace SimpleDB