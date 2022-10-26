#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_scan.h"

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


TEST(TableTest, TablePageTest) {
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

    std::unique_ptr<Transaction> tx 
        = std::make_unique<Transaction> (file_manager.get(), buf_manager.get(), rm.get());

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    std::vector<char> q(10,'1');
    std::vector<char> test2(15, '2');
    std::vector<char> test3(5, '3');
    Layout layout;
    TableScan ts(tx.get(), test_file, layout);

    Tuple tuple_test(q);
    Tuple tuple_test2(test2);
    Tuple tuple_test3(test3);
    Tuple tuple_get;
    

    int round = 1000;

    std::vector<RID> rid_vec;
    std::vector<RID> lost_vec;

    for (int i = 0;i < round;i ++) {
        ts.NextInsert(tuple_test);
        ts.Insert(tuple_test);
        rid_vec.push_back(ts.GetRid());
    }

    ts.FirstTuple();
    
    for (int i = 0;i < round;i ++) {
        ts.Next();
        EXPECT_EQ(rid_vec[i], ts.GetRid());
        Tuple t;
        assert(ts.GetTuple(&t) == true);
        EXPECT_EQ(t, tuple_test);
    }

    ts.FirstTuple();

    for (int i = 0;i < round;i ++) {
        ts.Next();
        
        if (find(std::to_string(i), '9')) {
            // std::cout << "be delete" << ts.GetRid().ToString() << std::endl;
            ts.Delete();
            lost_vec.push_back(ts.GetRid());
        }
    }

    ts.FirstTuple();
    while (ts.Next()) {
        if (find(std::to_string(ts.GetRid().GetSlot()), '8')) {
            ts.Update(tuple_test2);
            Tuple tuple;
            ts.GetTuple(&tuple);
            EXPECT_EQ(tuple, tuple_test2);
        }
    }    

    ts.FirstTuple();
    while (ts.Next()) {
        if (find(std::to_string(ts.GetRid().GetSlot()), '7')) {
            ts.Update(tuple_test3);
            Tuple tuple;
            ts.GetTuple(&tuple);
            EXPECT_EQ(tuple, tuple_test3);
        }
    }

    ts.FirstTuple();
    
    for (int i = 0;i < 5;i ++) {
        ts.NextInsert(tuple_test);
        ts.Insert(tuple_test);
        ts.GetTuple(&tuple_get);
        EXPECT_EQ(tuple_test, tuple_get);
        EXPECT_EQ(lost_vec[i], ts.GetRid());
    }

    tx->Commit();
    /// 
    /// @brief print
    ///   
    auto log_iter = log_manager->Iterator();
    while(log_iter.HasNextRecord()) {
        // auto byte_array1 = log_iter.CurrentRecord();
        
        // static_cast<InsertRecord*>
        // auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        // std::cout << log_record_tmp->ToString() << std::endl;
        log_iter.NextRecord();
    }
    
    
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}


TEST(TableTest, RandomTableTest) {

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

    std::unique_ptr<Transaction> tx 
        = std::make_unique<Transaction> (file_manager.get(), buf_manager.get(), rm.get());

    int min = 1000,max = 10000;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    std::vector<char> q(10,'1');
    std::vector<char> test2(15, '2');
    std::vector<char> test3(5, '3');
    Layout layout;

    TableScan ts(tx.get(), test_file, layout);



    int round = distrib(engine);
    round = 100;
    std::vector<RID> rid_vec;
    std::vector<Tuple> tuple_array;
    std::vector<RID> empty_rid_vec;
     ts.FirstTuple();
    for (int i = 0;i < round;i ++) {
        tuple_array.push_back(GetRandomTuple());
        ts.NextInsert(tuple_array[i]);
        ts.Insert(tuple_array[i]);
        rid_vec.push_back(ts.GetRid());
    }

    ts.FirstTuple();
    
    for (int i = 0;i < round;i ++) {
        ts.Next();
        EXPECT_EQ(rid_vec[i], ts.GetRid());
        Tuple t;
        assert(ts.GetTuple(&t) == true);
        EXPECT_EQ(t, tuple_array[i]);
    }

    ts.FirstTuple();
    int cnt = 0;
    for (int i = 0;i < round;i ++) {
        ts.Next();
        char c = '9';
        if (find(std::to_string(ts.GetRid().GetSlot()), c)) {
            // std::cout << "be delete" << ts.GetRid().ToString() << std::endl;
            ts.Delete();
            cnt++;
            empty_rid_vec.push_back(ts.GetRid());
        }
    }
    
    ts.FirstTuple();
    // find 
    for (auto t:empty_rid_vec) {
        std::cout << "find one " << t.ToString() << std::endl;
        auto tuple = FindTupleRID(tuple_array, t);
        ts.NextInsert(tuple);

    }


    ts.FirstTuple();
    while (ts.Next()) {
        if (find(std::to_string(ts.GetRid().GetSlot()), '8')) {
            Tuple random_tuple = GetRandomTuple();
            if (!ts.Update(random_tuple)) {
                std::cout << "stop" << std::endl;
                assert(false);
            }
            Tuple tuple;
            ts.GetTuple(&tuple);
            EXPECT_EQ(tuple, random_tuple);

            ModifyTupleWithRid(tuple_array, ts.GetRid(), random_tuple);
        }
    }    

    // ts.FirstTuple();
    // while (ts.Next()) {
    //     if (find(std::to_string(ts.GetRid().GetSlot()), '7')) {
    //         ts.Update(tuple_test3);
    //         Tuple tuple;
    //         ts.GetTuple(&tuple);
    //         EXPECT_EQ(tuple, tuple_test3);
    //     }
    // }

    tx->Commit();
    /// 
    /// @brief print
    ///   
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