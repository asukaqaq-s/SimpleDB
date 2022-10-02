#include "concurrency/transaction.h"
#include "buffer/buffer_manager.h"
#include "file/block_id.h"
#include "file/file_manager.h"
#include "file/page.h"
#include "log/log_manager.h"
#include "gtest/gtest.h"

#include <iostream>

namespace SimpleDB {
   

TEST(tx, transaction_test) {

  char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);
    
    std::unique_ptr<BufferManager> buffer_manager 
        = std::make_unique<BufferManager>(file_manager.get(), log_manager.get(), 10);

    auto tx1 = std::make_unique<Transaction>(file_manager.get(), log_manager.get(), buffer_manager.get());
    BlockId blk("testfile.txt", 0);
    buffer_manager->NewPage(blk.FileName());

    tx1->Pin(blk);    
    // The block initially contains unkown bytes
    // so don't log those values here
    tx1->SetInt(blk, 80, 1, false);
    tx1->SetString(blk, 40, "one", false);

    tx1->Commit();

    
    auto tx2 = std::make_unique<Transaction>(file_manager.get(), log_manager.get(), buffer_manager.get());
    
    tx2->Pin(blk);
    int iVal = tx2->GetInt(blk, 80);// return;
    std::string sVal = tx2->GetString(blk, 40);
    std::cout << "inital value at location 80 = " << iVal << std::endl;
    std::cout << "inital value at location 40 = " << sVal << std::endl;
    EXPECT_EQ(iVal, 1);
    EXPECT_EQ(sVal, "one");
    int newIVal = iVal + 1;
    std::string newSVal = sVal + "!";
    tx2->SetInt(blk, 80, newIVal, true);
    tx2->SetString(blk, 40, newSVal, true);
    tx2->Commit();
    
    auto tx3 = std::make_unique<Transaction>(file_manager.get(), log_manager.get(), buffer_manager.get());
    tx3->Pin(blk);
    std::cout << "new value at location 80 = " << tx3->GetInt(blk, 80)
                << std::endl;
    EXPECT_EQ(tx3->GetInt(blk, 80), newIVal);
    std::cout << "new value at location 40 = " << tx3->GetString(blk, 40)
                << std::endl;
    EXPECT_EQ(tx3->GetString(blk, 40), newSVal);
    tx3->SetInt(blk, 80, 9999, true);
    std::cout << "pre-rollback value at location 80 = " << tx3->GetInt(blk, 80)
                << std::endl;
    tx3->RollBack();
    
    auto tx4 = std::make_unique<Transaction>(file_manager.get(), log_manager.get(), buffer_manager.get());
    tx4->Pin(blk);
    std::cout << "post-rollback at location 80 = " << tx4->GetInt(blk, 80)
                << std::endl;
    EXPECT_EQ(tx4->GetInt(blk, 80),newIVal);
    tx4->Commit();

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

}