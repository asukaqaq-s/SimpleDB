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
    std::cout << "Tx A: request slock 1" << std::endl;
    ;
    txA->GetInt(blk1, 0);
    std::cout << "Tx A: receive slock 1" << std::endl;
    std::this_thread::sleep_for(1000ms);
    std::cout << "Tx A: request slock 2" << std::endl;
    txA->GetInt(blk2, 0);
    std::cout << "Tx A: receive slock 2" << std::endl;
    txA->Commit();
    std::cout << "Tx A: commit" << std::endl;
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
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
    std::cout << "Tx B: request xlock 2" << std::endl;
    ;
    txB->SetInt(blk2, 0, 0, false);
    std::cout << "Tx B: receive xlock 2" << std::endl;
    std::this_thread::sleep_for(1000ms);
    std::cout << "Tx B: request slock 1" << std::endl;
    txB->GetInt(blk1, 0);
    std::cout << "Tx B: receive slock 1" << std::endl;
    txB->Commit();
    std::cout << "Tx B: commit" << std::endl;
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
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
    std::cout << "Tx C: request xlock 1" << std::endl;
    ;
    txC->SetInt(blk1, 0, 0, false);
    std::cout << "Tx C: receive xlock 1" << std::endl;
    std::this_thread::sleep_for(1000ms);
    std::cout << "Tx C: request slock 2" << std::endl;
    txC->GetInt(blk2, 0);
    std::cout << "Tx C: receive slock 2" << std::endl;
    txC->Commit();
    std::cout << "Tx C: commit" << std::endl;
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
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

  A.join();
  B.join();
  C.join();

  cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

}
} // namespace simpledb
