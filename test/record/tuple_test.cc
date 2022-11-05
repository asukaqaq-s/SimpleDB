#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>


namespace SimpleDB {

TEST(TupleTest, SchemaTest) {
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

    std::string test_str = "12345678910";

    Tuple tuple;
    std::vector<Value> vec;

    // make schema
    Schema sch1;
    sch1.AddColumn(Column("a", TypeID::CHAR, 10));
    sch1.AddColumn(Column("b", TypeID::INTEGER));
    sch1.AddColumn(Column("c", TypeID::DECIMAL));
    sch1.AddColumn(Column("d", TypeID::VARCHAR, 20));
    // std::cout << sch1.ToString() << std::endl;

    Schema sch2;
    std::vector<Column> list 
    {   Column("a", TypeID::CHAR, 10), 
        Column("b", TypeID::INTEGER),
        Column("c", TypeID::DECIMAL),
        Column("d", TypeID::VARCHAR, 20)
    };
    sch2 = Schema(list);
    // std::cout << sch2.ToString() << std::endl;

    EXPECT_EQ(sch1, sch2);

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}


TEST(TupleTest, TupleTest) {
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


    std::string test_str = "12345678910";

    Tuple tuple;
    std::vector<Value> vec;

     // make schema
    Schema sch1;
    sch1.AddColumn(Column("a", TypeID::CHAR, 100));
    sch1.AddColumn(Column("b", TypeID::INTEGER));
    sch1.AddColumn(Column("c", TypeID::DECIMAL));
    sch1.AddColumn(Column("d", TypeID::VARCHAR, 120));
    sch1.AddColumn(Column("e", TypeID::INTEGER));

    
    vec.push_back(Value(test_str, TypeID::CHAR));
    vec.push_back(Value(10));
    vec.push_back(Value(121.0));
    vec.push_back(Value(test_str, TypeID::VARCHAR));
    vec.push_back(Value(100));



    tuple = Tuple(vec, sch1);
    

    // print tuple
    std::cout << tuple.GetSize() << std::endl;
    std::cout << tuple.ToString(sch1) << std::endl;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}






}