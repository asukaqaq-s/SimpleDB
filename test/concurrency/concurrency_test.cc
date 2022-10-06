#include <iostream>
#include <thread>

#include "buffer/buffer_manager.h"
#include "file/block_id.h"
#include "file/file_manager.h"
#include "file/page.h"
#include "log/log_manager.h"
#include "concurrency/transaction.h"
#include "gtest/gtest.h"

using namespace std::chrono_literals;

namespace SimpleDB {
void run_A(FileManager &fM, LogManager &lM, BufferManager &bM) {
  try {
    auto txA = std::make_unique<Transaction>(&fM, &lM, &bM);
    
    std::string testFile = "testfile";
    BlockId blk1(testFile, 1);
    BlockId blk2(testFile, 2);
    txA->Pin(blk1);
    txA->Pin(blk2);
    std::cout   << "Tx " << txA->GetTxnID() << ": request slock 1" << std::endl;
    ;
    txA->GetInt(blk1, 0);
    std::cout   << "Tx " << txA->GetTxnID() << ": receive slock 1" << std::endl;
    std::this_thread::sleep_for(1000ms);
    std::cout   << "Tx " << txA->GetTxnID() << ": request slock 2" << std::endl;
    txA->GetInt(blk2, 0);
    std::cout   << "Tx " << txA->GetTxnID() << ": receive slock 2" << std::endl;
    txA->Commit();
    std::cout   << "Tx " << txA->GetTxnID() << ": commit" << std::endl;
  } catch (std::exception &e) {
    std::cout  << "A " << e.what() << std::endl;
  }
}

void run_B(FileManager &fM, LogManager &lM, BufferManager &bM) {
  try {
    auto txB = std::make_unique<Transaction>(&fM, &lM, &bM);
    
    std::string testFile = "testfile";
    BlockId blk1(testFile, 1);
    BlockId blk2(testFile, 2);
    txB->Pin(blk1);
    txB->Pin(blk2);
    std::cout   << "Tx " << txB->GetTxnID() << ": request xlock 2" << std::endl;
    ;
    txB->SetInt(blk2, 0, 0, false);
    std::cout   << "Tx " << txB->GetTxnID() << ": receive xlock 2" << std::endl;
    std::this_thread::sleep_for(1000ms);
    std::cout   << "Tx " << txB->GetTxnID() << ": request slock 1" << std::endl;
    txB->GetInt(blk1, 0);
    std::cout   << "Tx " << txB->GetTxnID() << ": receive slock 1" << std::endl;
    txB->Commit();
    std::cout   << "Tx " << txB->GetTxnID() << ": commit" << std::endl;
  } catch (std::exception &e) {
    std::cout  << "B " << e.what() << std::endl;
  }
}

void run_C(FileManager &fM, LogManager &lM, BufferManager &bM) {
  try {
    auto txC = std::make_unique<Transaction>(&fM, &lM, &bM);
    
    std::string testFile = "testfile";
    BlockId blk1(testFile, 1);
    BlockId blk2(testFile, 2);
    txC->Pin(blk1);
    txC->Pin(blk2);
    std::this_thread::sleep_for(500ms);
    std::cout   << "Tx " << txC->GetTxnID() << ": request xlock 1" << std::endl;
    ;
    txC->SetInt(blk1, 0, 0, false);
    std::cout   << "Tx " << txC->GetTxnID() << ": receive xlock 1" << std::endl;
    std::this_thread::sleep_for(1000ms);
    std::cout   << "Tx " << txC->GetTxnID() << ": request slock 2" << std::endl;
    txC->GetInt(blk2, 0);
    std::cout   << "Tx " << txC->GetTxnID() << ": receive slock 2" << std::endl;
    txC->Commit();
    std::cout   << "Tx " << txC->GetTxnID() << ": commit" << std::endl;
  } catch (std::exception &e) {
    std::cout  << "C " << e.what() << std::endl;
  }
}
void run_D(FileManager &fM, LogManager &lM, BufferManager &bM) {
    try {
    auto txD = std::make_unique<Transaction>(&fM, &lM, &bM);
    
    std::string testFile = "testfile";
    BlockId blk1(testFile, 1);
    BlockId blk2(testFile, 2);
    txD->Pin(blk1);
    txD->Pin(blk2);
    std::this_thread::sleep_for(1000ms);
    std::cout   << "Tx " << txD->GetTxnID() << ": request slock 1" << std::endl;
    ;
    txD->GetInt(blk2, 0);
    std::this_thread::sleep_for(1500ms);
    std::cout   << "Tx " << txD->GetTxnID() << ": update slock 1" << std::endl;
    txD->SetInt(blk2, 0, 0, false);
    
    txD->Commit();
    
  } catch (std::exception &e) {
    std::cout  << "C " << e.what() << std::endl;
  }
}


TEST(tx, concurrency_test) {
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
    

    std::thread A(run_A, std::ref(fm), std::ref(lm), std::ref(bm));
    std::thread B(run_B, std::ref(fm), std::ref(lm), std::ref(bm));
    std::thread C(run_C, std::ref(fm), std::ref(lm), std::ref(bm));
    std::thread D(run_D, std::ref(fm), std::ref(lm), std::ref(bm));
    
    A.join();
    B.join();
    C.join();
    D.join();

    std::vector<std::thread> q;
    
    // for (int i = 0;i < 40;i ++) {
    //     q.push_back(std::thread (run_A, std::ref(fm), std::ref(lm), std::ref(bm)));
    // }
    
    // for (int i = 0;i < 40;i ++) {
    //     q[i].join();
    // }

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}
} // namespace simpledb
