#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "gtest/gtest.h"

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>

namespace SimpleDB {

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


TEST(LogRecordTest, InsertLogTest1) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    // produce log record
    
    std::map<int,int> mp;

    std::vector<char> log_record_vec;
    std::vector<InsertRecord> log_record_array;

    for(int i = 0;i < 1000;i ++) {
        int rid_x = distrib(engine),rid_y = distrib(engine);
        // ----------------------------------
        // | produce a random insert record |
        // ----------------------------------
        auto log_record = InsertRecord(1, test_file, RID(rid_x, rid_y), GetRandomTuple());
        
        
        log_record_array.push_back(log_record);
        log_record_vec = *log_record.Serializeto();
        auto log_record_tmp = LogRecord::DeserializeFrom(log_record_vec);
        auto tmp = dynamic_cast<InsertRecord*> (log_record_tmp.get());
        // std::cout << "tmp =          " << tmp->ToString() << std::endl;
        // std::cout << "log =          " << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record);

        // auto log_record = SetIntRecord(1, BlockId("text1.txt", 0), 10, old_value, new_value);
        log_manager->Append(log_record_vec);
    }    
    
    auto log_iter_main = log_manager->Iterator();
    int i = 0;
    while(log_iter_main.HasNextRecord()) {
        auto byte_array1 = log_iter_main.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        auto tmp = dynamic_cast<InsertRecord*> (log_record_tmp.get());
        // std::cout << tmp->ToString() << std::endl;
        // std::cout << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record_array[i]);

        i ++;
        log_iter_main.NextRecord();
    }


    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}


TEST(LogRecordTest, DeleteLogTest1) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    // produce log record
    
    std::map<int,int> mp;

    std::vector<char> log_record_vec;
    std::vector<DeleteRecord> log_record_array;

    for(int i = 0;i < 1000;i ++) {
        int rid_x = distrib(engine),rid_y = distrib(engine);
        // ----------------------------------
        // | produce a random insert record |
        // ----------------------------------
        // int offset;
        auto log_record = DeleteRecord(1, test_file, RID(rid_x, rid_y), GetRandomTuple());
        
        
        log_record_array.push_back(log_record);
        log_record_vec = *log_record.Serializeto();
        auto log_record_tmp = LogRecord::DeserializeFrom(log_record_vec);
        auto tmp = dynamic_cast<DeleteRecord*> (log_record_tmp.get());
        // std::cout << "tmp =          " << tmp->ToString() << std::endl;
        // std::cout << "log =          " << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record);

        // auto log_record = SetIntRecord(1, BlockId("text1.txt", 0), 10, old_value, new_value);
        log_manager->Append(log_record_vec);
        // mp[i] = offset;
    }    
    
    auto log_iter_main = log_manager->Iterator();
    int i = 0;
    while(log_iter_main.HasNextRecord()) {
        auto byte_array1 = log_iter_main.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        auto tmp = dynamic_cast<DeleteRecord*> (log_record_tmp.get());
        // std::cout << tmp->ToString() << std::endl;
        // std::cout << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record_array[i]);

        i ++;
        log_iter_main.NextRecord();
    }


    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}

TEST(LogRecordTest, UpdateLogTest1) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    // produce log record
    
    std::map<int,int> mp;

    std::vector<char> log_record_vec;
    std::vector<UpdateRecord> log_record_array;

    for(int i = 0;i < 1000;i ++) {
        int rid_x = distrib(engine),rid_y = distrib(engine);
        // ----------------------------------
        // | produce a random insert record |
        // ----------------------------------
        // int offset;
        auto log_record = UpdateRecord(1, test_file, RID(rid_x, rid_y), GetRandomTuple(), GetRandomTuple());
        
        
        log_record_array.push_back(log_record);
        log_record_vec = *log_record.Serializeto();
        auto log_record_tmp = LogRecord::DeserializeFrom(log_record_vec);
        auto tmp = dynamic_cast<UpdateRecord*> (log_record_tmp.get());
        // std::cout << "tmp =          " << tmp->ToString() << std::endl;
        // std::cout << "log =          " << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record);

        // auto log_record = SetIntRecord(1, BlockId("text1.txt", 0), 10, old_value, new_value);
        log_manager->Append(log_record_vec);
        // mp[i] = offset;
    }    
    
    auto log_iter_main = log_manager->Iterator();
    int i = 0;
    while(log_iter_main.HasNextRecord()) {
        auto byte_array1 = log_iter_main.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        auto tmp = dynamic_cast<UpdateRecord*> (log_record_tmp.get());
        // std::cout << tmp->ToString() << std::endl;
        // std::cout << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record_array[i]);

        i ++;
        log_iter_main.NextRecord();
    }


    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}

TEST(LogRecordTest, InitPageLogTest1) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    // produce log record
    
    std::map<int,int> mp;

    std::vector<char> log_record_vec;
    std::vector<InitPageRecord> log_record_array;

    for(int i = 0;i < 1000;i ++) {
        int x = distrib(engine);
        // ----------------------------------
        // | produce a random insert record |
        // ----------------------------------
        // int offset;
        auto log_record = InitPageRecord(1, test_file, x);
        
        log_record_array.push_back(log_record);
        log_record_vec = *log_record.Serializeto();
        auto log_record_tmp = LogRecord::DeserializeFrom(log_record_vec);
        auto tmp = dynamic_cast<InitPageRecord*> (log_record_tmp.get());
        // std::cout << "tmp =          " << tmp->ToString() << std::endl;
        // std::cout << "log =          " << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record);

        // auto log_record = SetIntRecord(1, BlockId("text1.txt", 0), 10, old_value, new_value);
        log_manager->Append(log_record_vec);
        // mp[i] = offset;
    }    
    
    auto log_iter_main = log_manager->Iterator();
    int i = 0;
    while(log_iter_main.HasNextRecord()) {
        auto byte_array1 = log_iter_main.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        auto tmp = dynamic_cast<InitPageRecord*> (log_record_tmp.get());
        // std::cout << tmp->ToString() << std::endl;
        // std::cout << log_record_.ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record_array[i]);

        i ++;
        log_iter_main.NextRecord();
    }


    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}



TEST(LogRecordTest, ChkptEndLogTest1) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    int min = -1e9,max = 1e9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    //////////// produce txn map ///////////
    
    


    // produce log record
    
    std::map<int,int> mp;

    std::vector<char> log_record_vec;
    std::vector<ChkptEndRecord> log_record_array;

    int log_num = 10;
    int mp_num = 10;

    for(int i = 0;i < log_num;i ++) {

        std::map<txn_id_t, TxTableEntry> txn_mp;

        for (int j = 0;j < mp_num;j ++) {
            int x = distrib(engine);
            int y = distrib(engine);
            txn_mp[x] = {y, TxStatus::U};
        }

        //////////// produce dp map ////////////
        
        std::map<BlockId, lsn_t> dp_mp;

        for (int j = 0;j < mp_num;j ++) {
            int x = distrib(engine);
            int y = distrib(engine);
            dp_mp[BlockId(test_file, x)] = y;
        }

        auto log_record = ChkptEndRecord(txn_mp, dp_mp);
        
        log_record_array.push_back(log_record);
        log_record_vec = *log_record.Serializeto();
        auto log_record_tmp = LogRecord::DeserializeFrom(log_record_vec);
        auto tmp = dynamic_cast<ChkptEndRecord*> (log_record_tmp.get());
        EXPECT_EQ(*tmp, log_record_array[i]);
        log_manager->Append(log_record_vec);
    }    
    
    auto log_iter_main = log_manager->Iterator();
    int i = 0;
    while(log_iter_main.HasNextRecord()) {
        auto byte_array1 = log_iter_main.CurrentRecord();
        
        // static_cast<InsertRecord*>
        auto log_record_tmp = (LogRecord::DeserializeFrom(byte_array1));
        auto tmp = dynamic_cast<ChkptEndRecord*> (log_record_tmp.get());
        // std::cout << tmp->ToString() << std::endl;
        // std::cout << log_record_.ToString() << std::endl;
        std::cout << "tmp  " << std::endl;
        std::cout << tmp->ToString() << std::endl;
        std::cout << "log_record  " << std::endl;
        std::cout << log_record_array[i].ToString() << std::endl;

        EXPECT_EQ(*tmp, log_record_array[i]);

        i ++;
        log_iter_main.NextRecord();
    }


    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}


} // namespace SimpleDB
