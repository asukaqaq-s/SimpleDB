#include "recovery/log_record.h"
#include "recovery/recovery_manager.h"
#include "gtest/gtest.h"
#include "concurrency/transaction.h"

#include <random>
#include <memory>

namespace SimpleDB {

int local_lsn = 0;

void WriteSetIntLog(txn_id_t txn_id, BlockId block, 
int offset, int old_value, int new_value, LogManager &lm) {
    auto log_record = SetIntRecord(txn_id, block, offset, old_value, new_value);
    log_record.SetPrevLSN(local_lsn);
    log_record.SetLsn(local_lsn++);
    lm.AppendLogRecord(log_record);
}

void WriteSetStringLog(txn_id_t txn_id, BlockId block, 
int offset, std::string old_value, std::string new_value, LogManager &lm) {
    auto log_record = SetStringRecord(txn_id, block, offset, old_value, new_value);
    log_record.SetPrevLSN(local_lsn);
    log_record.SetLsn(local_lsn++);
    lm.AppendLogRecord(log_record);
}


TEST(RecoveryTest, SimpleRollBackTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    BlockId test_block = BlockId(file_name, 0);
    FileManager fm(directory_path, block_size);
    LogManager lm(&fm, "log.log");
    BufferManager bm(&fm, &lm, 10);
    
    auto buffer = bm.NewPage(file_name);
    std::cout << buffer << std::endl;
    // return;
    std::unique_ptr<Transaction> tx1 = std::make_unique<Transaction>(&fm, &lm, &bm);
    std::unique_ptr<Transaction> tx2 = std::make_unique<Transaction>(&fm, &lm, &bm);
    
    /* tx 1, modify transaction */
    tx1->Pin(test_block);
    tx1->SetInt(test_block, 10, 20, true);
    tx1->SetInt(test_block, 10, 30, true);
    tx1->SetString(test_block, 50, "teststring now", true);
    tx1->SetString(test_block, 70, "teststring curr", true);
    
    /* tx2 is just read-only transaction */
    tx2->Pin(test_block);
    std::cout << "offset: 10 new-value: " << tx2->GetInt(test_block, 10) << std::endl;
    std::cout << "offset: 50 new-value: " << tx2->GetString(test_block, 50) << std::endl;
    std::cout << "offset: 70 new-value: " << tx2->GetString(test_block, 70) << std::endl;
    
    tx1->RollBack();
    // tx->Unpin(test_block); 
    std::cout << "offset: 10 old-value: " << tx2->GetInt(test_block, 10) << std::endl;
    std::cout << "offset: 50 old-value: " << tx2->GetString(test_block, 50) << std::endl;
    std::cout << "offset: 70 old-value: " << tx2->GetString(test_block, 70) << std::endl;
    
    tx2->Commit();

    auto log_iter = lm.Iterator();
    while(log_iter.HasNextRecord()) {
        std::cout << log_iter.GetLogOffset() << std::endl;
        auto byte_array = log_iter.CurrentRecord();
        auto log_record = LogRecord::DeserializeFrom(byte_array);
        std::cout << log_record->ToString() << std::endl;
        log_iter.NextRecord();
    }  


    std::string cmd = "rm -rf " + directory_path;
    
    system(cmd.c_str());

}


TEST(RecoveryTest, SimpleRecoverTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;

    std::string file_name = "test1.txt";


    std::string cmd1 = "rm -rf " + directory_path;
    
    system(cmd1.c_str());

    BlockId test_block = BlockId(file_name, 0);
    FileManager fm(directory_path, block_size);
    LogManager lm(&fm, "log.log");
    BufferManager bm(&fm, &lm, 10);
    
    

    auto buffer = bm.NewPage(file_name);
    bm.Unpin(buffer);
    // return;
    std::unique_ptr<Transaction> tx1 = std::make_unique<Transaction>(&fm, &lm, &bm);
    
    WriteSetIntLog(2, test_block, 10, 333, 20, lm);
    WriteSetIntLog(2, test_block, 30, 222, 30, lm);
    WriteSetIntLog(2, test_block, 40, 555, 40, lm);
    WriteSetIntLog(2, test_block, 50, 666, 50, lm);
    WriteSetIntLog(2, test_block, 100, 777, 1220, lm);
    WriteSetStringLog(2, test_block, 200, "qaq", "hukeman", lm);
    WriteSetStringLog(2, test_block, 300, "fff", "checkccpc", lm);
    // auto commit_record = CommitRecord(2);
    // lm.AppendLogRecord(commit_record);
    // return;
    tx1->Recovery();
    
    tx1->Pin(test_block);

    std::cout << test_block.to_string() << std::endl;
    
    EXPECT_EQ(tx1->GetInt(test_block, 10), 333);
    EXPECT_EQ(tx1->GetInt(test_block, 30), 222);
    EXPECT_EQ(tx1->GetInt(test_block, 40), 555);
    EXPECT_EQ(tx1->GetInt(test_block, 50), 666);
    EXPECT_EQ(tx1->GetInt(test_block, 100), 777);
    EXPECT_EQ(tx1->GetInt(test_block, 100), 777);
    EXPECT_EQ(tx1->GetString(test_block, 200), "qaq");
    EXPECT_EQ(tx1->GetString(test_block, 300), "fff");
    

    std::string cmd2 = "rm -rf " + directory_path;
    
    system(cmd2.c_str());

}

TEST(RecoveryTest, UndoRedoTEST) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;


    std::string cmd2 = "rm -rf " + directory_path;
    
    system(cmd2.c_str());

    std::string file_name = "test1.txt";
    BlockId test_block = BlockId(file_name, 0);
    FileManager fm(directory_path, block_size);
    LogManager lm(&fm, "log.log");
    BufferManager bm(&fm, &lm, 10);
    

    // random object
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    

    // test_buffer
    auto buffer = bm.NewPage(file_name);
    // bm.Unpin(buffer);
    
    // store tmp value
    std::vector<int> old_int_array(100);
    std::vector<int> new_int_array(100);
    std::vector<std::string> old_string_array(10);
    std::vector<std::string> new_string_array(10);

    // two transaction to vertify correctness
    std::unique_ptr<Transaction> tx1 = std::make_unique<Transaction>(&fm, &lm, &bm);
    std::unique_ptr<Transaction> tx2 = std::make_unique<Transaction>(&fm, &lm, &bm);


    int pos = 4;
    
    tx1->Pin(test_block);
    tx2->Pin(test_block);
    
    for(int i = 0;i < 100;i ++) {
        std::uniform_int_distribution<> k(-1e9, 1e9);
        int old_value = k(engine);
        int new_value = k(engine);

        buffer->contents()->SetInt(pos, old_value); 
        
        tx1->SetInt(test_block, pos, new_value, true);

        pos+= sizeof(int);
        old_int_array[i] = old_value;
        new_int_array[i] = new_value;
    }
    
    for(int i = 0;i < 10;i ++) {
        std::uniform_int_distribution<> k(0, 9);
        std::string old_string;
        std::string new_string;
        
        for(int j = 0;j < 10;j ++ ) {
            int old_value = k(engine) + '0';
            int new_value = k(engine) + '0';
            old_string += old_value;
            new_string += new_value;
        }

        buffer->contents()->SetString(pos, old_string); 
        tx1->SetString(test_block, pos, new_string, true);

        pos+= sizeof(int) + 10;
        old_string_array[i] = old_string;
        new_string_array[i] = new_string;
    }
    
    pos = 4;
    

    // test
    for(int i = 0;i < 100;i ++) {
        EXPECT_EQ(tx2->GetInt(test_block, pos), new_int_array[i]);
        pos +=4;
    }

    for(int i = 0;i < 10;i ++) {
        EXPECT_EQ(tx2->GetString(test_block, pos), new_string_array[i]);
        pos += 14;
    }

    // return;
    // test recovery
    tx2->Recovery();
    
    pos = 4;
    tx2->Pin(test_block);
    
    for(int i = 0;i < 100;i ++) {
        EXPECT_EQ(tx2->GetInt(test_block, pos),old_int_array[i] );
        pos += 4;
    }
    
    for(int i = 0;i < 10;i ++) {
        EXPECT_EQ(tx2->GetString(test_block, pos), old_string_array[i]);
        pos += 14;
    }
    
    // return;
    // test rollback
    tx1->RollBack();
    
    
    pos = 4;
    
    for(int i = 0;i < 100;i ++) {
        EXPECT_EQ(tx2->GetInt(test_block, pos), old_int_array[i]);
        pos += 4;
    }
    
    for(int i = 0;i < 10;i ++) {
        EXPECT_EQ(tx2->GetString(test_block, pos), old_string_array[i]);
        pos += 14;
    }

     std::string cmd3 = "rm -rf " + directory_path;
    
    system(cmd3.c_str());
}


TEST(RecoveryTest, CheckpointTEST) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;


    std::string cmd2 = "rm -rf " + directory_path;
    
    system(cmd2.c_str());

    std::string file_name = "test1.txt";
    BlockId test_block = BlockId(file_name, 0);
    FileManager fm(directory_path, block_size);
    LogManager lm(&fm, "log.log");
    BufferManager bm(&fm, &lm, 10);
    

    // random object
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    

    // test_buffer
    auto buffer = bm.NewPage(file_name);
    // bm.Unpin(buffer);
    
    // store tmp value
    std::vector<int> old_int_array(100);
    std::vector<int> new_int_array(100);
    std::vector<std::string> old_string_array(10);
    std::vector<std::string> new_string_array(10);

    // two transaction to vertify correctness
    std::unique_ptr<Transaction> tx1 = std::make_unique<Transaction>(&fm, &lm, &bm);
    tx1->Pin(test_block);
    
    WriteSetIntLog(2, test_block, 10, 333, 20, lm);
    WriteSetIntLog(2, test_block, 30, 222, 30, lm);
    WriteSetIntLog(2, test_block, 40, 555, 40, lm);
    WriteSetIntLog(2, test_block, 50, 666, 50, lm);
    WriteSetIntLog(2, test_block, 100, 777, 1220, lm);
    WriteSetStringLog(2, test_block, 200, "qaq", "hukeman", lm);
    WriteSetStringLog(2, test_block, 300, "fff", "checkccpc", lm);

    auto checkpoint_record = CheckpointRecord();

    lm.AppendLogRecord(checkpoint_record);
    
    EXPECT_NE(tx1->GetInt(test_block, 10), 333);
    EXPECT_NE(tx1->GetInt(test_block, 30), 222);
    EXPECT_NE(tx1->GetInt(test_block, 40), 555);
    EXPECT_NE(tx1->GetInt(test_block, 50), 666);
    EXPECT_NE(tx1->GetInt(test_block, 100), 777);
    EXPECT_NE(tx1->GetInt(test_block, 100), 777);
    EXPECT_NE(tx1->GetString(test_block, 200), "qaq");
    EXPECT_NE(tx1->GetString(test_block, 300), "fff");



    std::string cmd3 = "rm -rf " + directory_path;
    
    system(cmd3.c_str());
}



} // namespace SimpleDB