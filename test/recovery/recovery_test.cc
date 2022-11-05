#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"
#include "concurrency/transaction_manager.h"

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>



namespace SimpleDB {

bool find(std::string str, char c) {
    for (auto t:str) {
        if (t == c) {
            return true;
        }
    }
    return false;
}

Tuple GetRandomTuple() {
    
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(1, 100); // uniform distribution
    
    int size  = distrib(engine);
    std::vector<char> q;

    for (int i = 0; i < size;i ++) {
        std::uniform_int_distribution<> distrib('0', '9'); // uniform distribution
        q.push_back(distrib(engine));
    }

    return Tuple(q);

}

Tuple FindTupleRID(const std::vector<Tuple> &arr, RID rid) {
    for (auto t:arr) {
        if (t.GetRID() == rid) {
            return t;
        }
    }

}


void ModifyTupleWithRid(std::vector<Tuple> &arr, RID rid, Tuple new_tuple) {
    for (auto &t:arr) {
        if (t.GetRID() == rid) {
            t = new_tuple;
            return;
        }
    }
}


TEST(TransactionTest, SimpleRecoveryTest) {
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    std::cout << local_path << std::endl;
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

    auto lock = std::make_unique<LockManager>();
    TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
    RID rid;

    auto tx1 = std::move(txn_mgr.Begin());
    auto tx2 = std::move(txn_mgr.Begin());
    auto tx3 = std::move(txn_mgr.Begin());


    auto table_heap = std::make_unique<TableHeap>(tx1.get(), test_file, 
                      file_manager.get(), rm.get(), buf_manager.get());

    
    Tuple tuple1(std::vector<char>(10,'1'));
    Tuple tuple2(std::vector<char>(15,'2'));
    Tuple tuple3(std::vector<char>(5,'3'));
    

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution



    for (int i = 0;i < 100;i ++) {
        RID rid;
        table_heap->Insert(tx1.get(), tuple1, &rid);

        Tuple tuple;
        table_heap->GetTuple(tx1.get(), rid, &tuple);
        EXPECT_EQ(tuple1, tuple);
    }

    
    auto iter1 = table_heap->Begin(tx1.get());
    for (int i = 0;i < 100;i ++) {
        RID rid = iter1.GetRID();

        if (i % 2) {
            table_heap->Update(tx1.get(), rid, tuple2);

            Tuple tuple = iter1.Get();
            EXPECT_EQ(tuple2, tuple);
        }
        else {
            table_heap->Update(tx1.get(), rid, tuple3);

            Tuple tuple = iter1.Get();
            EXPECT_EQ(tuple3, tuple);
        }
        iter1++;
    }
    
    txn_mgr.Commit(tx1.get());

    
    
    for (int i = 0;i < 100;i ++) {
        table_heap->Insert(tx2.get(), tuple1, nullptr);
    }

    auto iter2 = table_heap->Begin(tx2.get());
    for (int i = 0;i < 100;i ++) {
        RID rid = iter2.GetRID();

        if (i % 2) {
            Tuple tuple = iter2.Get();
            EXPECT_EQ(tuple2, tuple);
            
            table_heap->Update(tx2.get(), rid, tuple1);
            
            tuple = iter2.Get();
            EXPECT_EQ(tuple1, tuple);
            
        } else {
            Tuple tuple = iter2.Get();
            EXPECT_EQ(tuple3, tuple);

            table_heap->Delete(tx2.get(), rid);
            EXPECT_EQ(false, table_heap->GetTuple(tx2.get(), rid, &tuple));
        }
        iter2++;
    }  
    

    rm->Recover(tx2.get());

    // tx2->Commit();
    // auto ts3 = TableHeap(tx3.get(), test_file, layout);
    
    iter2 = table_heap->Begin(tx2.get());
    for (int i = 0;i < 1000 && !iter2.IsEnd();i ++) {
        RID rid = iter2.GetRID();
        Tuple tuple = iter2.Get();
        
        if (i % 2) {
            EXPECT_EQ(tuple, tuple2);
        }
        else {
            EXPECT_EQ(tuple, tuple3);
        }

        iter2++;
    }

    auto log_iter = log_manager->Iterator();
    while(log_iter.HasNextRecord()) {
        auto byte_array1 = log_iter.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        std::cout << log_record_tmp->ToString() << std::endl;

        log_iter.NextRecord();
    }
    
        
    
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}





} // namespace SimpleDB