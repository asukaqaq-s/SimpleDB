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

    auto buffer = bfm->NewBlock(filename, nullptr);
    auto *bucket = reinterpret_cast<BPlusTreeBucketPage<GenericKey<4>, RID, GenericComparator<4>>*>
                                    (buffer->contents()->GetRawDataPtr());
    bucket->Init(0);

    for (int i = 0;i < 100;i++) {
        bucket->Insert({i,i});
    }

    EXPECT_EQ(bucket->GetSize(), 100);

    for (int i = 30;i < 50;i ++) {
        bucket->Remove({i, i});
    }

    EXPECT_EQ(bucket->GetSize(), 80);


}




} // namespace SimpleDB