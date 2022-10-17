
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"

namespace SimpleDB {

TEST(LogRecordTest, SetIntTest) {
    return;
    BlockId block("txt1.txt", 0);
    SetIntRecord origin_record(3, block, 80, 122, 0222);
    auto copy_record = origin_record;

    copy_record.SetCLR();
    origin_record.SetPrevLSN(100);
    origin_record.SetLsn(200);
    // std::cout << origin_record.ToString() << std::endl;
    // std::cout << copy_record.ToString() << std::endl;
    EXPECT_NE(copy_record, origin_record); 
    
    auto byte_array = origin_record.Serializeto();
    auto page = LogRecord::DeserializeFrom(*byte_array);
    EXPECT_EQ(page->GetLsn(),200);
    EXPECT_EQ(*(static_cast<SetIntRecord*>(page.get())), origin_record);
    
    // std::cout << origin_record.ToString() << std::endl;
    // std::cout << page->ToString() << std::endl;

    std::shared_ptr<Page> p = std::make_shared<Page>(byte_array);
    SetIntRecord test_record(p.get());
    
    EXPECT_EQ(test_record, origin_record);

}

TEST(LogRecordTest, SetStringTest) {

    BlockId block("txt1.txt", 0);
    SetStringRecord origin_record(3, block, 80, "12223", "445562");

    auto copy_record = origin_record;

    copy_record.SetCLR();
    origin_record.SetPrevLSN(100);
    origin_record.SetLsn(200);
    // std::cout << origin_record.ToString() << std::endl;
    // std::cout << copy_record.ToString() << std::endl;
    EXPECT_NE(copy_record, origin_record); 
    
    auto byte_array = origin_record.Serializeto();
    auto page = LogRecord::DeserializeFrom(*byte_array);
    EXPECT_EQ(page->GetLsn(),200);
    EXPECT_EQ(*(static_cast<SetStringRecord*>(page.get())), origin_record);
    
    // std::cout << origin_record.ToString() << std::endl;
    // std::cout << page->ToString() << std::endl;

    std::shared_ptr<Page> p = std::make_shared<Page>(byte_array);
    SetStringRecord test_record(p.get());
    
    EXPECT_EQ(test_record, origin_record);

}

TEST(LogRecordTest, LogRecordTest)  {
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;

    std::string cmd1 = "rm -rf " + directory_path;
    
    system(cmd1.c_str());

    std::string file_name = "test1.txt";
    BlockId test_block = BlockId(file_name, 0);
    FileManager fm(directory_path, block_size);
    LogManager lm(&fm, "log.log");
    BufferManager bm(&fm, &lm, 10);
    auto buffer = bm.NewPage(file_name);
    // return;
    // std::unique_ptr<Transaction> tx1 = std::make_unique<Transaction>(&fm, &lm, &bm);
    int MAXTIME = 10000;
    
    
    BlockId block("txt1.txt", 0);
    SetIntRecord log_record(3, block, 80, 122, 0222);
    auto log_vec = *log_record.Serializeto();
    log_record.SetPrevLSN(100);
    log_record.SetLsn(200);
    
    for (int i = 0;i < MAXTIME;i ++) {
        lm.AppendLogRecord(log_record);
    }
    int cnt = 0;
    auto logit = lm.Iterator();
    while (logit.HasNextRecord()) {
        auto vec = logit.CurrentRecord();
        auto record = LogRecord::DeserializeFrom(vec);
        record->SetLsn(MAXTIME - 1);
        EXPECT_EQ(record->ToString(), log_record.ToString());
        logit.NextRecord();
        cnt ++;
    }

    EXPECT_EQ(cnt, MAXTIME);
    

    std::string cmd2 = "rm -rf " + directory_path;
    
    system(cmd2.c_str());
}

} // namespace SimpleDB