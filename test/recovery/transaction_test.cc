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


TEST(TransactionTest, SimpleRALLBACKTest) {
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

    std::unique_ptr<Transaction> tx1 
        = std::make_unique<Transaction> (file_manager.get(), buf_manager.get(), rm.get());

    std::unique_ptr<Transaction> tx2
        = std::make_unique<Transaction> (file_manager.get(), buf_manager.get(), rm.get());

    std::unique_ptr<Transaction> tx3
        = std::make_unique<Transaction> (file_manager.get(), buf_manager.get(), rm.get());

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    std::vector<char> q(10,'1');
    std::vector<char> test2(15, '2');
    std::vector<char> test3(5, '3');
    Layout layout;
    Tuple tuple_test1(q);
    Tuple tuple_test2(test2);
    Tuple tuple_test3(test3);    
    
    TableScan ts1(tx1.get(), test_file, layout);
    ts1.FirstTuple();


    for (int i = 0;i < 10;i ++) {
        ts1.NextInsert(tuple_test1);
        ts1.Insert(tuple_test1);
    }

    std::cout << "file size = " << tx1->GetFileSize(test_file + ".table") << std::endl;
    tx1->Abort();
    std::cout << "file size = " << tx2->GetFileSize(test_file + ".table") << std::endl;
    

    // TableScan ts2(tx2.get(), test_file, layout);
    // TableScan ts3(tx3.get(), test_file, layout);
    // ts2.FirstTuple();
    // ts3.FirstTuple();

    
    // for (int i = 0;i < 10;i ++) {
    //     ts2.Next();
    //     Tuple tuple;
    //     EXPECT_EQ(ts2.GetTuple(&tuple), true);
    //     EXPECT_EQ(tuple, tuple_test1);
    //     ts2.Update(tuple_test2);
    //     EXPECT_EQ(ts2.GetTuple(&tuple), true);
    //     EXPECT_EQ(tuple, tuple_test2);
    // }

    // tx2->Abort();

    // for (int i = 0;i < 10;i ++) {
    //     ts3.Next();
    //     Tuple tuple;
    //     ts3.GetTuple(&tuple);
    //     EXPECT_EQ(tuple, tuple_test1);
    // }

    // tx3->Commit();
    

    auto log_iter = log_manager->Iterator();
    while(log_iter.HasNextRecord()) {
        auto byte_array1 = log_iter.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        std::cout << log_record_tmp->ToString() << std::endl;

        if (log_record_tmp->GetRecordType() == LogRecordType::INITPAGE && log_record_tmp->IsCLR()) {
            log_record_tmp->Redo(tx2.get());
            std::cout << "file size = " << tx2->GetFileSize(test_file + ".table") << std::endl;
        }

        log_iter.NextRecord();
    }
    
    
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}





} // namespace SimpleDB