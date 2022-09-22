#include "buffer/buffer_manager.h"
#include "file/block_id.h"
#include "file/file_manager.h"
#include "file/page.h"
#include "gtest/gtest.h"

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

std::mutex latch;

std::vector<char> GetRandomVector() {
    int min = 0,max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution
    std::vector<char> return_vec(4096);

    for(int i = 0;i < return_vec.size();i ++) {
        std::uniform_int_distribution<unsigned> k(0, 9);
        auto c = k(engine) + '0';
        // auto c = '1' + (i%8);
        return_vec[i] = c;
    }

    return return_vec;
}

void TestPin(std::map<BlockId,int> *mp, BlockId x) {
    std::lock_guard<std::mutex> lock(latch);
    // if(mp->find(x) == mp->end()) {
        (*mp)[x] ++;
    // }
}

void TestUnpin(std::map<BlockId,int> *mp, BlockId x) {
    std::lock_guard<std::mutex> lock(latch);
    if(mp->find(x) == mp->end()) {
        return;
    }
    (*mp)[x] --;
    if((*mp)[x] == 0) {
        mp->erase(x);
    }
}


/************************************************************
* test functions
*************************************************************/

TEST(BufferTest, Simpletest1) {
  // simpledb db("buffertest", 400, 3); // three buffers
    return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    FileManager *fm = new FileManager(directory_path, block_size);
    LogManager *lm = new LogManager(fm, "buffertest.log");
    BufferManager *bfm = new BufferManager(fm, lm, 5);
    
    int pin_times = 10;
    int const_be_buffered = 2;

    // txn_id_t txn = 1;
    std::vector<Buffer*> buffer_array;
    std::vector<BlockId> block_array;
    
    /* create new file */
    for(int i = 0;i < 5000;i ++) {
        bfm->NewPage(BlockId(file_name, i));
        bfm->Unpin(BlockId(file_name, i));
        block_array.push_back(BlockId(file_name, i));
    }
    
    EXPECT_EQ(bfm->available(), 5);

    /* increasing the pin_count */
    for(int i = 0;i < pin_times;i ++) {
        
        for(int j = 0;j < const_be_buffered;j ++) {
            auto buf = bfm->Pin(block_array[j]);
            buffer_array.push_back(buf);
        }
        
        for(int j = const_be_buffered;j < 500;j ++) {
            auto buf = bfm->Pin(block_array[j]);
            
            for(int k = 0;k < const_be_buffered;k ++) {
                EXPECT_NE(buf, buffer_array[k]);
            }

            bfm->Unpin(buf);
        }

        for(int j = 0;j < const_be_buffered;j ++) {
            EXPECT_EQ(i + 1, buffer_array[j]->GetPinCount());
        }
    }

    EXPECT_EQ(bfm->available(), 5 - const_be_buffered);


    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());

}


TEST(BufferPoolManagerTest, BinaryDataTest) {
    return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 98;

    FileManager *fm = new FileManager(directory_path, block_size);
    LogManager *lm = new LogManager(fm, "buffertest.log");
    BufferManager *bfm = new BufferManager(fm, lm, buffer_pool_size);

    auto *page0 = bfm->NewPage(file_name);
    auto data_ = page0->contents()->content();
    
    // Scenario: The buffer pool is empty. We should be able to create a new page.
    ASSERT_NE(nullptr, page0);
    EXPECT_EQ(0, page0->BlockNum().BlockNum());

    // Scenario: Once we have a page, we should be able to read and write content.
    snprintf(&((*data_)[0]), sizeof(data_), "Hello");

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    for (size_t i = 1; i < buffer_pool_size; ++i) {
        EXPECT_NE(nullptr, bfm->NewPage(BlockId(file_name, i)));
    }

    // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
    // there would still be one cache frame left for reading page 0.
    for (int i = 0; i < 5; ++i) {
        bfm->Unpin(BlockId(file_name, i));
    }
    for (int i = 0; i < 4; ++i) {
        EXPECT_NE(nullptr, bfm->NewPage(file_name));
    }

    // Scenario: We should be able to fetch the data we wrote a while ago.
    page0 = bfm->Pin(BlockId(file_name, 0));
    bfm->Unpin(BlockId(file_name, 0));


    /// @param  
    std::string cmd = "rm -rf " + directory_path;
    system(cmd.c_str());
}


TEST(BufferPoolManagerTest, ConcurrencyWriteTest) {
    return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 110;

    const int num_threads = 10;
    const int num_runs = 500;

    for(int run = 0; run < num_runs; run++) {
        FileManager *fm = new FileManager(directory_path, block_size);
        LogManager *lm = new LogManager(fm, "buffertest.log");
        BufferManager *bpm = new BufferManager(fm, lm, buffer_pool_size);

        std::vector<std::thread> threads;
        
        for(int tid = 0; tid < num_threads; tid ++) {
            std::mutex lock;
            threads.push_back(std::thread([&bpm]() {
                
                std::vector<BlockId> page_ids = std::vector<BlockId>(10);
                int block_size = 4 * 1024;

                for(int i = 0;i < 10;i ++) {
                    BlockId page_id;
                    std::string file_name = "test1.txt";
                    auto new_buffer = bpm->NewPage(file_name);
                    
                    EXPECT_NE(nullptr, new_buffer);
                    ASSERT_NE(nullptr, new_buffer);
                    
                    auto data_ = ((new_buffer->contents()->content()));
                    page_id = new_buffer->BlockNum();

                    std::string str = page_id.FileName() + std::to_string(page_id.BlockNum());
                    
                    for(int j = 0;j < str.size();j ++)
                       (*data_)[j] = str[j];
                    
                    new_buffer->SetModified(1, 0);
                    new_buffer->flush(); /* REMEMBER: flush into disk, otherwise can not access next test */
                    page_ids[i] = (page_id);
                }
                
                bpm->FlushAll(1);
                for (int i = 0; i < 10; i++) {
                    bpm->Unpin(page_ids[i]);
                }
                
                for (int i = 0; i < 10;i ++) {
                    auto page = bpm->Pin(page_ids[i]);
                    EXPECT_NE(nullptr, page);
                    ASSERT_NE(nullptr, page);
                    auto data_ = ((page->contents()->content()));
                    BlockId page_id = page->BlockNum();

                    std::string str = page_id.FileName() + std::to_string(page_id.BlockNum());
                    for(int j = 0;j < str.size();j ++) {
                        EXPECT_EQ(str[j], (*data_)[j]);
                    }

                    bpm->Unpin(page_ids[i]);
                }

            }));
        }
        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        
        /// delete  
        std::string cmd = "rm -rf " + directory_path;
        system(cmd.c_str());
        delete fm;
        delete bpm;
        delete lm;
    }
}


TEST(BufferPoolManagerTest, ConcurrencyTest) {
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 500;

    const int num_threads = 5;
    const int num_runs = 10;
    const int one_thread_control = 100;

    for(int run = 0; run < num_runs; run++) {
        FileManager *fm = new FileManager(directory_path, block_size);
        LogManager *lm = new LogManager(fm, "buffertest.log");
        BufferManager *bpm = new BufferManager(fm, lm, buffer_pool_size);

        std::vector<std::thread> threads;
        
        for(int i = 0;i < num_threads * one_thread_control;i ++) {
            bpm->NewPage(BlockId(file_name, i));
        }
        
        std::vector<std::vector<char>> arr = 
            std::vector<std::vector<char>> (num_threads * one_thread_control);

        for(int tid = 0; tid < num_threads; tid ++) {
            threads.push_back(std::thread([&bpm](int num, std::vector<std::vector<char>> *arr) {
                std::vector<BlockId> page_ids = std::vector<BlockId>(one_thread_control);
                std::string file_name = "test1.txt";
                int block_size = 4 * 1024;

                for(int i = 0;i < one_thread_control;i ++) {
                    auto page = bpm->Pin(BlockId(file_name, i + num * one_thread_control));

                    *page->contents()->content() = GetRandomVector();
                    (*arr)[num * one_thread_control + i] = *page->contents()->content();
                    page->SetModified(1, 0);
                    page->flush();
                }
                bpm->FlushAll(1);

                for(int i = num * one_thread_control;i < (num + 1 ) * one_thread_control;i ++) {
                    bpm->Unpin(BlockId(file_name, i));
                }

            }, tid, &arr));
        }
        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }

        //*****test*****//
        for(int i = 0;i < num_threads * one_thread_control;i ++) {
            auto page = bpm->Pin(BlockId(file_name, i));
            EXPECT_EQ(*page->contents()->content(), arr[i]);
        }
        // pause();
        /// delete  
        std::string cmd = "rm -rf " + directory_path;
        system(cmd.c_str());
        delete fm;
        delete bpm;
        delete lm;
    }
}

TEST(BufferPoolManagerTest, ConcurrencyPinTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 500;

    const int num_threads = 5;
    const int num_runs = 10;
    const int one_thread_control = 100;

    for(int run = 0; run < num_runs; run++) {
        FileManager *fm = new FileManager(directory_path, block_size);
        LogManager *lm = new LogManager(fm, "buffertest.log");
        BufferManager *bpm = new BufferManager(fm, lm, buffer_pool_size);

        std::vector<std::thread> threads;
        std::map<BlockId, int> count_;

        for(int i = 0;i < num_threads * one_thread_control;i ++) {
            bpm->NewPage(BlockId(file_name, i));
            bpm->Unpin(BlockId(file_name, i));
        }
    
        for(int tid = 0; tid < num_threads; tid ++) {
            threads.push_back(std::thread([&bpm](int num, std::map<BlockId, int> *count) {
                std::random_device seed; // get ramdom seed
                std::ranlux48 engine(seed());
                std::string file_name = "test1.txt";

                for(int i = 0;i < 500;i ++) {
                    std::uniform_int_distribution<unsigned> k(3, 9);
                    int c = k(engine);
                    (*count)[BlockId(file_name, i)] += c;
                    
                    for(int j = 0;j < c;j ++) {
                        bpm->Pin(BlockId(file_name, i));    
                    }
                }

                for(int i = 0;i < 500;i ++) {
                    std::uniform_int_distribution<unsigned> k(0, 3);
                    int c = k(engine);
                    (*count)[BlockId(file_name, i)] -= c;
                    
                    for(int j = 0;j < c;j ++) {
                        bpm->Unpin(BlockId(file_name, i));    
                    }
                }

            }, tid, &count_));
        }
        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        
        for (int i = 0;i < 500;i ++) {
            auto page = bpm->Pin(BlockId(file_name, i));
            bpm->Unpin(BlockId(file_name, i));
            EXPECT_EQ(page->GetPinCount(), (count_[BlockId(file_name, i)]));
        }

        // pause();
        /// delete  
        std::string cmd = "rm -rf " + directory_path;
        system(cmd.c_str());
        delete fm;
        delete bpm;
        delete lm;
    }
}


TEST(BufferPoolManagerTest, ConcurrencyVictimTest) {
    // return;
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 9);

    const int buffer_pool_size = 100;

    const int num_threads = 1;
    const int num_runs = 1000;
    // const int one_thread_control = 10;

    for(int run = 0; run < num_runs; run++) {
        FileManager *fm = new FileManager(directory_path, block_size);
        LogManager *lm = new LogManager(fm, "buffertest.log");
        BufferManager *bpm = new BufferManager(fm, lm, buffer_pool_size);

        std::vector<std::thread> threads;
        std::map<BlockId, int> count_;

        for(int i = 0;i < buffer_pool_size;i ++) {
            bpm->NewPage(BlockId(file_name, i));
            bpm->Unpin(BlockId(file_name, i));
        }
    
        for(int tid = 0; tid < num_threads; tid ++) {
            threads.push_back(std::thread([&bpm](int num, std::map<BlockId, int> *count) {
                std::random_device seed; // get ramdom seed
                std::ranlux48 engine(seed());
                std::string file_name = "test1.txt";
                std::uniform_int_distribution<unsigned> k(10000, 20000);
                int simluated_round = k(engine);
                
                for(int i = 0;i < simluated_round;i ++) {
                    // randomly simulated a request
                    std::uniform_int_distribution<unsigned> select_victim(0, 99);
                    BlockId victim = BlockId(file_name, select_victim(engine));
                    
                    if(i & 1) {
                        TestPin(count, victim);
                        bpm->Pin(victim);
                        // std::unique_lock<std::mutex> lock(latch);
                        // std::cout << "thread " << num << " pin victim " << victim.BlockNum()<< std::endl;
                        // std::cout << std::endl;
                        // lock.unlock();
                    } else {
                        TestUnpin(count, victim);
                        bpm->Unpin(victim);
                        // std::unique_lock<std::mutex> lock(latch);
                        // std::cout << "thread " << num << " unpin victim " << victim.BlockNum()<< std::endl;
                        // std::cout << std::endl;
                        // l/ock.unlock();
                    }
                    
                }

            }, tid, &count_));
        }
        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        
        for (int i = 0;i < buffer_pool_size;i ++) {
            auto page = bpm->Pin(BlockId(file_name, i));
            bpm->Unpin(BlockId(file_name, i));
            EXPECT_EQ(page->GetPinCount(), (count_[BlockId(file_name, i)]));
        }

        // pause();
        /// delete  
        std::string cmd = "rm -rf " + directory_path;
        system(cmd.c_str());
        delete fm;
        delete bpm;
        delete lm;
    }
}


} // namespace simpledb
