/************************************************************
** this test file is focus on replace algorithm
*************************************************************/


#include "buffer/buffer_manager.h"
#include "file/block_id.h"
#include "file/file_manager.h"
#include "file/page.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"

#include <execinfo.h>
#include <iostream>
#include <random>
#include <queue>
#include <cstring>
#include <sstream>
#include <map>

namespace SimpleDB {

/************************************************************
* help functions
*************************************************************/


/************************************************************
* test functions
*************************************************************/

TEST(BufferManagerTest, Simpletest1) {
  // simpledb db("buffertest", 400, 3); // three buffers
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 500;
    const int num_page = 5000;
    
    FileManager *fm = new FileManager(directory_path, block_size);
    LogManager *lm = new LogManager(fm, "buffertest.log");
    RecoveryManager *rm = new RecoveryManager(lm);
    BufferManager *bfm = new BufferManager(fm, rm, buffer_pool_size);
    
    int pin_times = 10;
    int const_be_buffered = 2;

    // txn_id_t txn = 1;
    std::vector<Buffer*> buffer_array;
    std::vector<BlockId> block_array;
    
    /* create new file */
    for(int i = 0;i < 5000;i ++) {
        auto page = bfm->NewPage(BlockId(file_name, i));
        bfm->UnpinBlock(BlockId(file_name, i));
        buffer_array.push_back(page);
    }
    
    for(int i = 0;i < buffer_pool_size;i ++) {
        bfm->PinBlock(BlockId(file_name, i));
    }
    
    for(int i = 0;i < buffer_pool_size;i ++) {
        bfm->UnpinBlock(BlockId(file_name, i));
        auto page = bfm->PinBlock(BlockId(file_name,i + buffer_pool_size));
        EXPECT_EQ(page, buffer_array[i]);
    }
    
    for(int i = 0;i < 10;i ++) {
        bfm->UnpinBlock(BlockId(file_name, i + buffer_pool_size));
    }
    auto page = bfm->PinBlock(BlockId(file_name, 10 + 2 * buffer_pool_size));
    EXPECT_EQ(page, buffer_array[0]);
    bfm->UnpinBlock(page);

    for(int i = 10;i < buffer_pool_size;i ++) {
        bfm->UnpinBlock(buffer_array[i]);
    }

    /* pin 0 ~ 100 in bufferpool, and vectim should not be that one from  0 ~ 100 */
    
    for(int i = 0;i < 100;i ++) {
        auto page = bfm->PinBlock(BlockId(file_name,i));
    }

    for(int i = 100;i < 500;i ++) {
        auto page = bfm->PinBlock(BlockId(file_name,i));
        EXPECT_EQ(page, buffer_array[i]);
        for(int j = 0;j < 100;j ++) {
            EXPECT_NE(page, buffer_array[j]);
        }
    }


    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());

    delete fm;
    delete rm;
    delete bfm;
    delete lm;

}

TEST(BufferManagerTest, VictimTest) {
  // simpledb db("buffertest", 400, 3); // three buffers
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 500;
    const int num_page = 5000;
    
    FileManager *fm = new FileManager(directory_path, block_size);
    LogManager *lm = new LogManager(fm, "buffertest.log");
    RecoveryManager *rm = new RecoveryManager(lm);
    BufferManager *bfm = new BufferManager(fm, rm, buffer_pool_size);
    
    int pin_times = 10;
    int const_be_buffered = 2;

    // txn_id_t txn = 1;
    std::vector<Buffer*> buffer_array;
    std::vector<BlockId> block_array;
    
    /* create new file */
    for(int i = 0;i < 5000;i ++) {
        auto page = bfm->NewPage(BlockId(file_name, i));
        bfm->UnpinBlock(BlockId(file_name, i));
        buffer_array.push_back(page);
    }
    
    for(int i = 0;i < 100;i ++) {
        bfm->PinBlock(BlockId(file_name, i));
        bfm->PinBlock(BlockId(file_name, i));
    }

    /*
    * note that, we pin twice in 0 ~ 100, so the next pin function should not return
    * their buffer to be victim
    */
    
    for(int i = 0;i < 2;i ++) {
        for(int j = 100;j < 500;j ++) {
            auto page = bfm->PinBlock(BlockId(file_name, j));
            for(int k = 0;k < 100;k ++) {
                EXPECT_NE(page, buffer_array[k]);
            }
            bfm->UnpinBlock(BlockId(file_name, j));
        }
        for(int k = 0;k < 100;k ++) {
            bfm->UnpinBlock(BlockId(file_name, k));
        }
    }

    for(int i = 0;i < 400;i ++) {
        auto page = bfm->PinBlock(BlockId(file_name, i));
        int j;
        for(j = 0;j < 500;j ++) {
            if(buffer_array[j] == page)
                break;
        }
    }

    for(int i = 0;i < 100;i ++) {
        auto page = bfm->PinBlock(BlockId(file_name, i));   
        EXPECT_EQ(page, buffer_array[i + 100]); // 0 ~ 100 is in bufferpool    
    }

    for(int i = 400;i < 500;i ++) {
        auto page = bfm->PinBlock(BlockId(file_name, i));   
        EXPECT_EQ(page, buffer_array[i - 400]); // now, the buffer 0 ~ 100 is unpinned 
    }

    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());


    delete fm;
    delete rm;
    delete bfm;
    delete lm;
}


TEST(BufferManagerTest, BinaryDataTest) {
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::random_device rd;
    std::mt19937 mt(rd());
    // generate random number range from 0 to max
    std::uniform_int_distribution<char> dis(0);

    const int buffer_pool_size = 10;
    const int num_page = 5000;
    const int PAGE_SIZE = 4096;
    
    FileManager *fm = new FileManager(directory_path, block_size);
    LogManager *lm = new LogManager(fm, "buffertest.log");
    RecoveryManager *rm = new RecoveryManager(lm);
    BufferManager *bpm = new BufferManager(fm, rm, buffer_pool_size);

    BlockId block_id[buffer_pool_size * 2];
    auto page0 = bpm->NewPage(file_name);
    block_id[0] = BlockId(file_name, 0);

    // we shouldn't have any assumption on the page number, since it's decided by disk manager
    // buffer pool is empty, we should be able to create a new page
    EXPECT_NE(nullptr, page0);

    char random_binary_data[PAGE_SIZE];
    // Generate random binary data
    for (char &i : random_binary_data) {
        i = dis(mt);
    }

    // insert terminal characters both in the middle and at end
    random_binary_data[PAGE_SIZE / 2] = '\0';
    random_binary_data[PAGE_SIZE - 1] = '\0';

    std::memcpy(page0->contents()->GetRawDataPtr(), random_binary_data, PAGE_SIZE);
    EXPECT_EQ(std::memcmp(page0->contents()->GetRawDataPtr(), random_binary_data, PAGE_SIZE), 0);

    // we should be able to allocate there pages
    for (size_t i = 1; i < buffer_pool_size; i++) {
        block_id[i] = BlockId(file_name, i);
        EXPECT_NE(nullptr, bpm->NewPage(file_name));
    }

    // we should not be able to allocate any new pages
    for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; i++) {
        EXPECT_EQ(nullptr, bpm->NewPage(file_name));
    }

    // unpin 5 pages and allocate 5 new pages
    // which will lead the previous pages swapped out to disk
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(bpm->UnpinBlock(block_id[i], true), true);
    }

    BlockId more_pages[5];
    for (int i = 0; i < 5; i++) {
        auto *buffer = bpm->NewPage(file_name);
        EXPECT_NE(buffer, nullptr);
        more_pages[i] = buffer->GetBlockID();
        bpm->UnpinBlock(more_pages[i]);
    }

    // fetch first page from disk again
    page0 = bpm->PinBlock(block_id[0]);
    EXPECT_EQ(0, std::memcmp(page0->contents()->GetRawDataPtr(), random_binary_data, PAGE_SIZE));
    EXPECT_EQ(true, bpm->UnpinBlock(block_id[0], true));

    delete fm;
    delete rm;
    delete bpm;
    delete lm;

    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());
}

TEST(BufferManagerTest, ConcurrentTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 8;
    const size_t worker_size = 6;
    const size_t total_page_size = 10;
    const size_t iteration_num = 10;

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::random_device rd;
    std::mt19937 mt(rd());
    // generate random number range from 0 to max
    std::uniform_int_distribution<char> dis(0);

    const int num_page = 5000;
    const int PAGE_SIZE = 4096;
    
    FileManager *fm = new FileManager(directory_path, block_size);
    LogManager *lm = new LogManager(fm, "buffertest.log");
    RecoveryManager *rm = new RecoveryManager(lm);
    BufferManager *bpm = new BufferManager(fm, rm, buffer_pool_size);

    std::vector<BlockId> page_list(total_page_size);
    // allocate total_page_size pages
    for (size_t i = 0; i < total_page_size; i++) {
        page_list[i] = BlockId(file_name, i);
        EXPECT_NE(bpm->NewPage(file_name), nullptr);
        EXPECT_EQ(bpm->UnpinBlock(page_list[i], false), true);
    }

    const size_t BEGIN_OFFSET = 0;
    const size_t MIDDLE_OFFSET = PAGE_SIZE / 2;
    const size_t END_OFFSET = PAGE_SIZE - sizeof(int);

    std::vector<std::thread> worker_list;
    for (size_t i = 0; i < worker_size; i++) {
        worker_list.emplace_back(std::thread([&]() {
            // thread local random engine
            std::random_device rd;
            std::mt19937 mt(rd());
            std::vector<BlockId> access_list = page_list;
            
            for (size_t i = 0; i < iteration_num; i++) {
                // shuffle the access list
                std::shuffle(access_list.begin(), access_list.end(), mt);

                for (auto block_id : access_list) {
                    // increase the counter that is located at three area
                    auto page = bpm->PinBlock(block_id);
                    // first lock this page
                    page->WLock();
                    int *begin_ptr = reinterpret_cast<int *> (page->contents()->GetRawDataPtr() + BEGIN_OFFSET);
                    *begin_ptr = *begin_ptr + 1;
                    int *middle_ptr = reinterpret_cast<int *> (page->contents()->GetRawDataPtr()  + MIDDLE_OFFSET);
                    *middle_ptr = *middle_ptr + 1;
                    int *end_ptr = reinterpret_cast<int *> (page->contents()->GetRawDataPtr()  + END_OFFSET);
                    *end_ptr = *end_ptr + 1;
                    page->WUnlock();
                    // unpin the page and mark it dirty
                    bpm->UnpinBlock(block_id, true);
                }
            }
        }));
    }

    // wait for the thread
    for (size_t i = 0; i < worker_size; i++) {
        worker_list[i].join();
    }

    // check the value
    for (auto block_id : page_list) {
        auto page = bpm->PinBlock(block_id);
        EXPECT_NE(page, nullptr);
        int *begin_ptr = reinterpret_cast<int *> (page->contents()->GetRawDataPtr()  + BEGIN_OFFSET);
        EXPECT_EQ(*begin_ptr, iteration_num * worker_size);
        int *middle_ptr = reinterpret_cast<int *> (page->contents()->GetRawDataPtr()  + MIDDLE_OFFSET);
        EXPECT_EQ(*middle_ptr, iteration_num * worker_size);
        int *end_ptr = reinterpret_cast<int *> (page->contents()->GetRawDataPtr()  + END_OFFSET);
        EXPECT_EQ(*end_ptr, iteration_num * worker_size);
        bpm->UnpinBlock(block_id, false);
    }

    delete fm;
    delete rm;
    delete bpm;
    delete lm;

    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());
}




} // namespace SimpleDB