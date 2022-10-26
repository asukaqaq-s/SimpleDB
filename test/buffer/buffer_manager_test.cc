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
        bfm->Unpin(BlockId(file_name, i));
        buffer_array.push_back(page);
    }
    
    for(int i = 0;i < buffer_pool_size;i ++) {
        bfm->Pin(BlockId(file_name, i));
    }
    
    for(int i = 0;i < buffer_pool_size;i ++) {
        bfm->Unpin(BlockId(file_name, i));
        auto page = bfm->Pin(BlockId(file_name,i + buffer_pool_size));
        EXPECT_EQ(page, buffer_array[i]);
    }
    
    for(int i = 0;i < 10;i ++) {
        bfm->Unpin(BlockId(file_name, i + buffer_pool_size));
    }
    auto page = bfm->Pin(BlockId(file_name, 10 + 2 * buffer_pool_size));
    EXPECT_EQ(page, buffer_array[0]);
    bfm->Unpin(page);

    for(int i = 10;i < buffer_pool_size;i ++) {
        bfm->Unpin(buffer_array[i]);
    }

    /* pin 0 ~ 100 in bufferpool, and vectim should not be that one from  0 ~ 100 */
    
    for(int i = 0;i < 100;i ++) {
        auto page = bfm->Pin(BlockId(file_name,i));
    }

    for(int i = 100;i < 500;i ++) {
        auto page = bfm->Pin(BlockId(file_name,i));
        EXPECT_EQ(page, buffer_array[i]);
        for(int j = 0;j < 100;j ++) {
            EXPECT_NE(page, buffer_array[j]);
        }
    }

    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());

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
        bfm->Unpin(BlockId(file_name, i));
        buffer_array.push_back(page);
    }
    
    for(int i = 0;i < 100;i ++) {
        bfm->Pin(BlockId(file_name, i));
        bfm->Pin(BlockId(file_name, i));
    }

    /*
    * note that, we pin twice in 0 ~ 100, so the next pin function should not return
    * their buffer to be victim
    */
    
    for(int i = 0;i < 2;i ++) {
        for(int j = 100;j < 500;j ++) {
            auto page = bfm->Pin(BlockId(file_name, j));
            for(int k = 0;k < 100;k ++) {
                EXPECT_NE(page, buffer_array[k]);
            }
            bfm->Unpin(BlockId(file_name, j));
        }
        for(int k = 0;k < 100;k ++) {
            bfm->Unpin(BlockId(file_name, k));
        }
    }

    for(int i = 0;i < 400;i ++) {
        auto page = bfm->Pin(BlockId(file_name, i));
        int j;
        for(j = 0;j < 500;j ++) {
            if(buffer_array[j] == page)
                break;
        }
    }

    for(int i = 0;i < 100;i ++) {
        auto page = bfm->Pin(BlockId(file_name, i));   
        EXPECT_EQ(page, buffer_array[i + 100]); // 0 ~ 100 is in bufferpool    
    }

    for(int i = 400;i < 500;i ++) {
        auto page = bfm->Pin(BlockId(file_name, i));   
        EXPECT_EQ(page, buffer_array[i - 400]); // now, the buffer 0 ~ 100 is unpinned 
    }

    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());

}



} // namespace SimpleDB