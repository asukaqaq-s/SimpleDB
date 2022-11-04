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

    std::string test_str = "12345678910";

    Tuple tuple;
    std::vector<Value> vec;

    // make schema
    Schema sch;
    sch.AddField("a", TypeID::INTEGER, 0);
    sch.AddField("b", TypeID::VARCHAR, 100);
    sch.AddField("c", TypeID::CHAR, 100);
    sch.AddField("d", TypeID::DECIMAL, 0);

    Layout layout(sch);

    vec.push_back(Value(10));
    vec.push_back(Value(test_str));
    vec.push_back(Value(test_str, TypeID::CHAR));
    vec.push_back(Value(121.0));

    {
        std::cout << layout.GetOffset("a") << std::endl;
        std::cout << layout.GetOffset("b") << std::endl;
        std::cout << layout.GetOffset("c") << std::endl;
        std::cout << layout.GetOffset("d") << std::endl;
    }

    tuple = Tuple(vec, layout);
    
    std::cout << tuple.GetSize() << std::endl;

    {
        std::cout << tuple.GetValue("a", layout).to_string() << std::endl;
        std::cout << tuple.GetValue("b", layout).to_string() << std::endl;
        std::cout << tuple.GetValue("c", layout).to_string() << std::endl;
        std::cout << tuple.GetValue("d", layout).to_string() << std::endl;
    }

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

}