#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "gtest/gtest.h"

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>

namespace SimpleDB {

/***********************************************************************************
* TEST  FUNCTIONS 
************************************************************************************/
std::vector<char> CreateLogRecord(int cnt) {
    int min = 0,max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution
    
    std::string log_record = "Record " + std::to_string(cnt) + " : ";
    int random_size = distrib(engine) + 100;
    int size = log_record.size();
    for(int i = 0;i < random_size - size;i ++) {
        std::uniform_int_distribution<unsigned> k(0, 9);
        auto c = k(engine) + '0'; 
        log_record += c;
    }
    return std::vector<char> (log_record.begin(), log_record.end());
}

std::vector<std::vector<char>> CreateLogs(LogManager * lm,int times) {
    std::vector<std::vector<char>> res;
    for(int i = 0;i < times;i ++) {
        std::vector<char> log_record = CreateLogRecord(i);
        int x = lm->Append(log_record);
        // std::cout << "lsn = " << x << std::endl;
        res.push_back(log_record);
    }
    return res;
}

int get_file_size(const char* filename)
{
    std::ifstream in(filename);
    if(!in.is_open()) return 0;
    in.seekg(0, std::ios_base::end);
    std::streampos sp = in.tellg();
    
    return sp;
}

/***********************************************************************************
* TEST  CLASS 
************************************************************************************/


TEST(LogManagerTest, EasyTest1) {
    
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    /**************************************************************
    * start test!!!! 
    ***************************************************************/
    

    std::vector<std::vector<char>> log_records; //= CreateLogs(log_manager.get(), times);
    
    for(int i = 0;i < 1000;i ++) {
        char c = i % 10 + '0';
        std::vector<char> test_vector(10,c);
        log_manager->Append(test_vector);
        log_records.push_back(test_vector);
    }
    // return;
    std::reverse(log_records.begin(), log_records.end()); // for debugging purpose
    auto log_iterator = log_manager->Iterator();
    
    
    for(int i = 0;i < 1000;i ++) {
        std::vector<char> array1 = log_iterator.NextRecord();
        std::vector<char> array2 = log_records[i];
        EXPECT_EQ(array1, array2);    
    }
    // pause();
    /**************************************************************
    * end test!!!! 
    ***************************************************************/
    // delete all database file for testing 
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

TEST(LogManagerTest, RandomTest1) {
    // return;
    for(int i = 0;i < 100;i ++ ) {
        char buf[100];
        std::string local_path = getcwd(buf, 100);
        std::string test_dir = local_path + "/" + "test_dir";
        std::string cmd;
        std::string log_file_name = "log.log";
        std::string log_file_path = test_dir + "/" + log_file_name;
        std::unique_ptr<FileManager> file_manager 
            = std::make_unique<FileManager>(test_dir, 4096);
        
        std::unique_ptr<LogManager> log_manager 
            = std::make_unique<LogManager>(file_manager.get(), log_file_name);
        
        /**************************************************************
        * start test!!!! 
        ***************************************************************/
        
        int times = 1000;

        auto log_records = CreateLogs(log_manager.get(), times);
        std::reverse(log_records.begin(), log_records.end()); // for debugging purpose
        auto log_iterator = log_manager->Iterator();

        // sleep(20);
        int time = 0;
        for(int i = 0;i < times;i ++) {
            std::vector<char> array1 = log_iterator.NextRecord();
            std::vector<char> array2 = log_records[i];
            // for(auto t:array1) std::cout <<t<< std::flush;
            // std::cout << std::endl;
            // for(auto t:array2) std::cout <<t<< std::flush;
            // std::cout << std::endl;
            
            EXPECT_EQ(array1, array2);    
            // if(array1 != array2) time++;
            // if(time == 10) 
            if(array1 != array2)
                pause();
        }
        
        /**************************************************************
        * end test!!!! 
        ***************************************************************/
        // pause();
        // delete all database file for testing 
        cmd = "rm -rf " + test_dir;
        system(cmd.c_str());

        // std::cout << std::endl << std::endl;
    }
}


TEST(LogManagerTest, RandomTest2) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    /**************************************************************
    * start test
    ***************************************************************/

    int times = 1679;
    
    auto log_records = CreateLogs(log_manager.get(), times);
    std::reverse(log_records.begin(), log_records.end()); 
    auto log_iterator = log_manager->Iterator();
    
    for(int i = 0;i < times;i ++) {
        std::vector<char> array1 = log_iterator.NextRecord();
        std::vector<char> array2 = log_records[i];
        EXPECT_EQ(array1, array2);    
    }
    
    /**************************************************************
    * end test
    ***************************************************************/
    
    // delete all database file for testing 
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}


TEST(LogManagerTest, RandomTest3) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    /**************************************************************
    * start test
    ***************************************************************/

    int times = 100;
    
    for(int i = 0;i < 100;i ++ ) {
        
        auto log_records = CreateLogs(log_manager.get(), times);
        std::reverse(log_records.begin(), log_records.end()); 
        auto log_iterator = log_manager->Iterator();
        
        for(int i = 0;i < times;i ++) {
            std::vector<char> array1 = log_iterator.NextRecord();
            std::vector<char> array2 = log_records[i];
            EXPECT_EQ(array1, array2);    
        }
    }
    /**************************************************************
    * end test
    ***************************************************************/
    
    // delete all database file for testing 
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

TEST(LogManagerTest, FlushTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    /**************************************************************
    * start test
    ***************************************************************/

    int times = 100;
    std::vector<std::vector<char>> total_log_records;
    
    for(int i = 0;i < 100;i ++ ) {
        int lsn = 0;
        for(int j = 0;j < times;j ++) {
            std::vector<char> log_record = CreateLogRecord(i);
            lsn = log_manager->Append(log_record);
            total_log_records.push_back(log_record);
        }
        log_manager->Flush(lsn);
    }
    std::reverse(total_log_records.begin(), total_log_records.end());
    auto log_iterator = log_manager->Iterator();
    for(int i = 0;i < times * 100;i ++) {
            std::vector<char> array1 = log_iterator.NextRecord();
            std::vector<char> array2 = total_log_records[i];
            EXPECT_EQ(array1, array2);
            // assert(array1 == array2);    
    }
    /**************************************************************
    * end test
    ***************************************************************/
    
    // delete all database file for testing 
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

TEST(LogManagerTest, FixSizeLogTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    /**************************************************************
    * start test
    ***************************************************************/

    int times = 100;
    std::vector<std::vector<char>> total_log_records;
    
    for(int i = 0;i < 100;i ++ ) {
        int lsn = 0;
        for(int j = 0;j < times;j ++) {
            std::string log_record_string;
            
            // append to log_record
            for(int k = 0;k < 10;k ++ ) {
                log_record_string += (k + '0');
            }
            
            std::vector<char> log_record = std::vector<char>
                    (log_record_string.begin(), log_record_string.end());
        
            lsn = log_manager->Append(log_record);
            total_log_records.push_back(log_record);
        }
        log_manager->Flush(lsn);
    }
    std::reverse(total_log_records.begin(), total_log_records.end());
    auto log_iterator = log_manager->Iterator();
    
    for(int i = 0;i < times * 100;i ++) {
        std::vector<char> array1 = log_iterator.NextRecord();
        std::vector<char> array2 = total_log_records[i];
        EXPECT_EQ(array1, array2);
        EXPECT_EQ(array1.size(), 10);
    }
    /**************************************************************
    * end test
    ***************************************************************/
    
    // delete all database file for testing 
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}


TEST(LogManagerTest, AppendTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    /**************************************************************
    * start test
    ***************************************************************/
    std::vector<std::vector<char>> total_log_records;
    int block_size = 4096;
    int log_size = 40;
    
    for(int i = 0;i < 30;i ++) {
        int one_block_log_number = (block_size - 4)/ (log_size + 4);
        for(int j = 0;j < one_block_log_number;j ++) {
            std::vector<char> log_record(log_size ,'0');
            int x = log_manager->Append(log_record);
            log_manager->Flush(x);
        }
        
        int block_num = get_file_size(log_file_path.c_str()) / block_size;
        EXPECT_EQ(block_num, i + 1);
    }

    /**************************************************************
    * end test
    ***************************************************************/
    
    // delete all database file for testing 
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}


} // namespace SimpleDB
