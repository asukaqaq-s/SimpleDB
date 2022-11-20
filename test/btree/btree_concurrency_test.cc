// #include "log/log_iterator.h"
// #include "log/log_manager.h"
// #include "recovery/log_record.h"
// #include "buffer/buffer_manager.h"
// #include "gtest/gtest.h"
// #include "recovery/recovery_manager.h"
// #include "record/table_heap.h"
// #include "record/table_iterator.h"
// #include "concurrency/transaction_manager.h"
// #include "index/btree/b_plus_tree_leaf_page.h"
// #include "index/btree/b_plus_tree_directory_page.h"
// #include "index/btree/b_plus_tree.h"


// #include <iostream>
// #include <memory>
// #include <random>
// #include <string>
// #include <cstring>
// #include <algorithm>
// #include <future>

// namespace SimpleDB {
// // helper function to launch multiple threads
// template <typename... Args>
// void LaunchParallelTest(uint num_threads, Args &&...args) {
//   std::vector<std::thread> thread_group;

//   // Launch a group of threads
//   for (uint thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
//     thread_group.push_back(std::thread(args..., thread_itr));
//   }

//   // Join the threads with the main thread
//   for (uint thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
//     thread_group[thread_itr].join();
//   }
// }

// // helper function to insert
// void InsertHelper(BPlusTree<GenericKey<4>, RID, GenericComparator<4>> *tree, const std::vector<int> &keys,
//                   __attribute__((unused)) uint64_t thread_itr = 0) {
//   GenericKey<4> index_key;
//   RID rid;
//   // create transaction
  
//   for (auto key : keys) {
//     RID rid(key, key);
//     index_key.SetFromInteger(key);
//     tree->Insert(index_key, rid);
//   }
// }

// // helper function to seperate insert
// void InsertHelperSplit(BPlusTree<GenericKey<4>, RID, GenericComparator<4>> *tree, const std::vector<int> &keys,
//                        int total_threads, __attribute__((unused)) uint thread_itr) {
//   GenericKey<4> index_key;
//   RID rid;
//   // create transaction
  
//   for (auto key : keys) {
//     if (static_cast<uint>(key) % total_threads == thread_itr) {
//       RID rid (key, key);
//       index_key.SetFromInteger(key);
//       tree->Insert(index_key, rid);
//     }
//   }

// }

// // helper function to delete
// void DeleteHelper(BPlusTree<GenericKey<4>, RID, GenericComparator<4>> *tree, const std::vector<int> &remove_keys,
//                   __attribute__((unused)) uint thread_itr = 0) {
//   GenericKey<4> index_key;
//   // create transaction
  
//   for (auto key : remove_keys) {
//     RID rid(key, key);
//     index_key.SetFromInteger(key);
//     tree->Remove(index_key, rid);
//   }

// }

// // helper function to seperate delete
// void DeleteHelperSplit(BPlusTree<GenericKey<4>, RID, GenericComparator<4>> *tree,
//                        const std::vector<int> &remove_keys, int total_threads,
//                        __attribute__((unused)) uint thread_itr) {
//   GenericKey<4> index_key;
//   // create transaction
  
//   for (auto key : remove_keys) {
//     if (static_cast<uint>(key) % total_threads == thread_itr) {
//       index_key.SetFromInteger(key);
//       tree->Remove(index_key, {key, key});
//     }
//   }

// }

// TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest1) {
//   const std::string filename = "test.db";
//     char buf[100];
//     std::string local_path = getcwd(buf, 100);
//     std::string test_dir = local_path + "/" + "test_dir";
//     std::string test_file = "test1.txt";
//     std::string cmd;
//     cmd = "rm -rf " + test_dir;
//     system(cmd.c_str());

//     std::string log_file_name = "log.log";
//     std::string log_file_path = test_dir + "/" + log_file_name;
//     std::unique_ptr<FileManager> fm 
//         = std::make_unique<FileManager>(test_dir, 4096);
    
//     std::unique_ptr<LogManager> lm 
//         = std::make_unique<LogManager>(fm.get(), log_file_name);

//     std::unique_ptr<RecoveryManager> rm 
//         = std::make_unique<RecoveryManager>(lm.get());

//     std::unique_ptr<BufferManager> bfm
//         = std::make_unique<BufferManager>(fm.get(), rm.get(), 100);

//     // --------------------
//     //  create execution context
//     // --------------------
//     auto lock = std::make_unique<LockManager> ();
//     TransactionManager txn_mgr(std::move(lock), rm.get(), fm.get(), bfm.get());


//     auto colA = Column("colA", TypeID::INTEGER);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     auto schema = Schema(cols);

    
//     GenericComparator<4> comparator(&schema);
//     BPlusTree<GenericKey<4>, RID, GenericComparator<4>> btree("basic_test", INVALID_BLOCK_NUM, 
//                                                              comparator, bfm.get());
//     GenericKey<4> index_key;
//     // create and fetch header_page
//     auto header_page = bfm->NewBlock(filename);
//     (void)header_page;
//     // keys to Insert
//     std::vector<int> keys;
//     int scale_factor = 100;
//     for (int key = 1; key < scale_factor; key++) {
//         keys.push_back(key);
//     }
//     LaunchParallelTest(2, InsertHelper, &btree, keys);

//     std::vector<RID> rids;
//     GenericKey<4> index_key;
//     for (auto key : keys) {
//         rids.clear();
//         index_key.SetFromInteger(key);
//         btree.GetValue(index_key, &rids);
//         EXPECT_EQ(rids.size(), 1);

        
//         EXPECT_EQ(rids[0].GetSlot(), key);
//     }



//     int start_key = 1;
//     int current_key = start_key;
//     index_key.SetFromInteger(start_key);
//     for (auto iterator = btree.Begin(index_key); iterator != btree.End(); ++iterator) {
//         auto location = (*iterator).second;
//         EXPECT_EQ(location.GetPageId(), 0);
//         EXPECT_EQ(location.GetSlotNum(), current_key);
//         current_key = current_key + 1;
//     }

//     EXPECT_EQ(current_key, keys.size() + 1);

//     bfm->UnpinPage(HEADER_PAGE_ID, true);
//     delete disk_manager;
//     delete bfm;
//     remove("test.db");
//     remove("test.log");
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<4> comparator(key_schema.get());
//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bfm = new BufferPoolManagerInstance(50, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<4>, RID, GenericComparator<4>> tree("foo_pk", bfm, comparator);
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bfm->NewBlock(&page_id);
//   (void)header_page;
//   // keys to Insert
//   std::vector<int> keys;
//   int scale_factor = 100;
//   for (int key = 1; key < scale_factor; key++) {
//     keys.push_back(key);
//   }
//   LaunchParallelTest(2, InsertHelperSplit, &btree, keys, 2);

//   std::vector<RID> rids;
//   GenericKey<4> index_key;
//   for (auto key : keys) {
//     rids.clear();
//     index_key.SetFromInteger(key);
//     btree.GetValue(index_key, &rids);
//     EXPECT_EQ(rids.size(), 1);

    
//     EXPECT_EQ(rids[0].GetSlotNum(), value);
//   }

//   int start_key = 1;
//   int current_key = start_key;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = btree.Begin(index_key); iterator != btree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//   }

//   EXPECT_EQ(current_key, keys.size() + 1);

//   bfm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bfm;
//   remove("test.db");
//   remove("test.log");
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<4> comparator(key_schema.get());

//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bfm = new BufferPoolManagerInstance(50, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<4>, RID, GenericComparator<4>> tree("foo_pk", bfm, comparator);
//   GenericKey<4> index_key;
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bfm->NewBlock(&page_id);
//   (void)header_page;
//   // sequential insert
//   std::vector<int> keys = {1, 2, 3, 4, 5};
//   InsertHelper(&btree, keys);

//   std::vector<int> remove_keys = {1, 5, 3, 4};
//   LaunchParallelTest(2, DeleteHelper, &btree, remove_keys);

//   int start_key = 2;
//   int current_key = start_key;
//   int size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = btree.Begin(index_key); iterator != btree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 1);

//   bfm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bfm;
//   remove("test.db");
//   remove("test.log");
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<4> comparator(key_schema.get());

//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bfm = new BufferPoolManagerInstance(50, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<4>, RID, GenericComparator<4>> tree("foo_pk", bfm, comparator);
//   GenericKey<4> index_key;
//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bfm->NewBlock(&page_id);
//   (void)header_page;

//   // sequential insert
//   std::vector<int> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//   InsertHelper(&btree, keys);

//   std::vector<int> remove_keys = {1, 4, 3, 2, 5, 6};
//   LaunchParallelTest(2, DeleteHelperSplit, &btree, remove_keys, 2);

//   int start_key = 7;
//   int current_key = start_key;
//   int size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = btree.Begin(index_key); iterator != btree.End(); ++iterator) {
//     auto location = (*iterator).second;
//     EXPECT_EQ(location.GetPageId(), 0);
//     EXPECT_EQ(location.GetSlotNum(), current_key);
//     current_key = current_key + 1;
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 4);

//   bfm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bfm;
//   remove("test.db");
//   remove("test.log");
// }

// TEST(BPlusTreeConcurrentTest, DISABLED_MixTest) {
//   // create KeyComparator and index schema
//   auto key_schema = ParseCreateStatement("a bigint");
//   GenericComparator<4> comparator(key_schema.get());

//   DiskManager *disk_manager = new DiskManager("test.db");
//   BufferPoolManager *bfm = new BufferPoolManagerInstance(50, disk_manager);
//   // create b+ tree
//   BPlusTree<GenericKey<4>, RID, GenericComparator<4>> tree("foo_pk", bfm, comparator);
//   GenericKey<4> index_key;

//   // create and fetch header_page
//   page_id_t page_id;
//   auto header_page = bfm->NewBlock(&page_id);
//   (void)header_page;
//   // first, populate index
//   std::vector<int> keys = {1, 2, 3, 4, 5};
//   InsertHelper(&btree, keys);

//   // concurrent insert
//   keys.clear();
//   for (int i = 6; i <= 10; i++) {
//     keys.push_back(i);
//   }
//   LaunchParallelTest(1, InsertHelper, &btree, keys);
//   // concurrent delete
//   std::vector<int> remove_keys = {1, 4, 3, 5, 6};
//   LaunchParallelTest(1, DeleteHelper, &btree, remove_keys);

//   int start_key = 2;
//   int size = 0;
//   index_key.SetFromInteger(start_key);
//   for (auto iterator = btree.Begin(index_key); iterator != btree.End(); ++iterator) {
//     size = size + 1;
//   }

//   EXPECT_EQ(size, 5);

//   bfm->UnpinPage(HEADER_PAGE_ID, true);
//   delete disk_manager;
//   delete bfm;
//   remove("test.db");
//   remove("test.log");
// }

// }  // namespace bustub
