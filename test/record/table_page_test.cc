#include "record/table_page.h"
#include "gtest/gtest.h"
#include "buffer/buffer_manager.h"
#include "file/file_manager.h"
#include "log/log_manager.h"

#include <random>
#include <memory>

namespace SimpleDB {

TEST(TablePageTest, SimpleTest) {

    int min = 0,max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution
    

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::string testFile = "testfile";
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    FileManager fm(test_dir, 4096);
    
    LogManager lm(&fm, log_file_name);
    
    BufferManager bm(&fm, &lm, 10);

    bm.NewPage(BlockId(testFile, 0));
    bm.NewPage(BlockId(testFile, 1));
    bm.NewPage(BlockId(testFile, 2));

    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &lm, &bm); 
    
    Schema sch = Schema();
    sch.AddIntField("A");
    sch.AddStringField("B", 9);
    Layout layout = Layout(sch);

    TablePage tp(txn.get(), BlockId(testFile, 0), layout);
    tp.InitPage();

    int slot = tp.NextEmptyTuple(-1);
    int cnt = 0;

    std::vector<int> iarr;
    std::vector<std::string> sarr;

    std::uniform_int_distribution<unsigned> k(100, 3210);
    while (slot >= 0) {  
        int n = k(engine);
        tp.SetInt(slot, "A", n);
        tp.SetString(slot, "B", "rec" + std::to_string(n));
        slot = tp.NextEmptyTuple(slot);
        cnt ++;
        iarr.push_back(n);
        sarr.push_back("rec" + std::to_string(n));
    }
    int cnt2 = 0;
    slot = tp.NextTuple(-1);

    while (slot >= 0) {
        // std::cout << tp.GetInt(slot, "A") << std::endl;
        // std::cout << tp.GetString(slot, "B") <<std::endl; 
    
        EXPECT_EQ(tp.GetInt(slot, "A"), iarr[cnt2]);
        EXPECT_EQ(tp.GetString(slot, "B"), sarr[cnt2]);
        cnt2++;
        slot = tp.NextTuple(slot);
    }

    for(int i = 0;i < 10;i ++)
        tp.MarkDeleteTuple(i);

    cnt2 = 0;
    slot = tp.NextTuple(-1);
    while (slot >= 0) {
        cnt2++;
        slot = tp.NextTuple(slot);
    }
    EXPECT_EQ(cnt, cnt2 + 10);
    
    for (int i = 100;i < 110;i ++) 
        tp.MarkDeleteTuple(i);
    
    slot = tp.NextTuple(-1);
    while (slot >= 0) {
        cnt2++;
        
        std::cout << slot << std::endl;
        slot = tp.NextTuple(slot);
        
        for (int i = 100;i < 110;i ++) 
            EXPECT_NE(slot, 100);
    }
    
    slot = -1;
    
    for (int i = 0;i < 10;i ++) {
        slot = tp.NextEmptyTuple(-1);
        EXPECT_EQ(slot, i);   
    }

    for (int i = 100;i < 110;i ++) {
        slot = tp.NextEmptyTuple(-1);
        EXPECT_EQ(slot, i);
    }
    

}


} // namespace SimpleDB