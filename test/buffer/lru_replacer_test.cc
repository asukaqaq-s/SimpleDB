/**
 * @file lru_replacer_test.cpp
 * @author sheep
 * @brief unit test for lru replacer
 * @version 0.1
 * @date 2022-04-30
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include <gtest/gtest.h>

#include <thread>
#include "buffer/lru_replace.h"
#include <random>
#include <algorithm>

namespace SimpleDB {

/**
 * @brief test from bustub
 */
TEST(LRUReplacerTest, SimpleTest) {
    // currently num_pages is invalid
    auto lru = LRUReplacer(0);

    lru.Unpin(1);
    lru.Unpin(2);
    lru.Unpin(3);
    lru.Unpin(4);
    lru.Unpin(5);
    lru.Unpin(6);
    lru.Unpin(1);
    EXPECT_EQ(6, lru.Size());
    // get Evicts from lru
    int value;
    lru.Evict(&value);
    EXPECT_EQ(1, value);
    lru.Evict(&value);
    EXPECT_EQ(2, value);
    lru.Evict(&value);
    EXPECT_EQ(3, value);
    // pin frames in replacer
    // since 3 is evicted, so pin 3 should have no effect
    lru.Pin(3);
    lru.Pin(4);
    EXPECT_EQ(2, lru.Size());

    lru.Unpin(4);
    
    // evict them all
    // we expect to see 5, 6, 4 since we have used 4 recently
    lru.Evict(&value);
    EXPECT_EQ(5, value);
    lru.Evict(&value);
    EXPECT_EQ(6, value);
    lru.Evict(&value);
    EXPECT_EQ(4, value);

    EXPECT_EQ(false, lru.Evict(&value));

    EXPECT_EQ(0, lru.Size());
}

/**
 * @brief you won't believe this test case is from leetcode
 * 
 */
TEST(LRUReplacerTest, SimpleTest2) {
    // currently num_pages is invalid
    auto lru = LRUReplacer(0);

    int value;

    lru.Unpin(1);
    lru.Unpin(2);
    EXPECT_EQ(2, lru.Size());

    // mimic that we have used frame 1
    lru.Pin(1);
    lru.Unpin(1);

    // evict 2
    lru.Evict(&value);
    EXPECT_EQ(2, value);

    // insert 3
    lru.Unpin(3);
    EXPECT_EQ(2, lru.Size());

    // evict 1
    lru.Evict(&value);
    EXPECT_EQ(1, value);

    // insert 4
    lru.Unpin(4);
    EXPECT_EQ(2, lru.Size());

    lru.Evict(&value);
    EXPECT_EQ(3, value);

    lru.Evict(&value);
    EXPECT_EQ(4, value);

    EXPECT_EQ(false, lru.Evict(&value));
}

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());

  // Scenario: get three Evicts from the clock.
  int value;
  lru_replacer.Evict(&value);
  EXPECT_EQ(1, value);
  lru_replacer.Evict(&value);
  EXPECT_EQ(2, value);
  lru_replacer.Evict(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been Evictized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);
  lru_replacer.Pin(4);
  EXPECT_EQ(2, lru_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);

  // Scenario: continue looking for Evicts. We expect these Evicts.
  lru_replacer.Evict(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Evict(&value);
  EXPECT_EQ(6, value);
  lru_replacer.Evict(&value);
  EXPECT_EQ(4, value);
}

TEST(LRUReplacerTest, Evict) {
  auto lru_replacer = new LRUReplacer(1010);

  // Empty and try removing
  int result;
  EXPECT_EQ(0, lru_replacer->Evict(&result)) << "Check your return value behavior for LRUReplacer::Evict";

  // Unpin one and remove
  lru_replacer->Unpin(11);
  EXPECT_EQ(1, lru_replacer->Evict(&result)) << "Check your return value behavior for LRUReplacer::Evict";
  EXPECT_EQ(11, result);

  // Unpin, remove and verify
  lru_replacer->Unpin(1);
  lru_replacer->Unpin(1);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(1, result);
  lru_replacer->Unpin(3);
  lru_replacer->Unpin(4);
  lru_replacer->Unpin(1);
  lru_replacer->Unpin(3);
  lru_replacer->Unpin(4);
  lru_replacer->Unpin(10);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(3, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(4, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(1, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(10, result);
  EXPECT_EQ(0, lru_replacer->Evict(&result)) << "Check your return value behavior for LRUReplacer::Evict";

  lru_replacer->Unpin(5);
  lru_replacer->Unpin(6);
  lru_replacer->Unpin(7);
  lru_replacer->Unpin(8);
  lru_replacer->Unpin(6);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(5, result);
  lru_replacer->Unpin(7);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(6, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(7, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(8, result);
  EXPECT_EQ(0, lru_replacer->Evict(&result)) << "Check your return value behavior for LRUReplacer::Evict";
  lru_replacer->Unpin(10);
  lru_replacer->Unpin(10);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(10, result);
  EXPECT_EQ(0, lru_replacer->Evict(&result));
  EXPECT_EQ(0, lru_replacer->Evict(&result));
  EXPECT_EQ(0, lru_replacer->Evict(&result));

  for (int i = 0; i < 1000; i++) {
    lru_replacer->Unpin(i);
  }
  for (int i = 10; i < 1000; i++) {
    EXPECT_EQ(1, lru_replacer->Evict(&result));
    EXPECT_EQ(i - 10, result);
  }
  EXPECT_EQ(10, lru_replacer->Size());

  delete lru_replacer;
}

TEST(LRUReplacerTest, Pin) {
  auto lru_replacer = new LRUReplacer(1010);

  // Empty and try removing
  int result;
  lru_replacer->Pin(0);
  lru_replacer->Pin(1);

  // Unpin one and remove
  lru_replacer->Unpin(11);
  lru_replacer->Pin(11);
  lru_replacer->Pin(11);
  EXPECT_EQ(false, lru_replacer->Evict(&result));
  lru_replacer->Pin(1);
  EXPECT_EQ(false, lru_replacer->Evict(&result));

  // Unpin, remove and verify
  lru_replacer->Unpin(1);
  lru_replacer->Unpin(1);
  lru_replacer->Pin(1);
  EXPECT_EQ(false, lru_replacer->Evict(&result));
  lru_replacer->Unpin(3);
  lru_replacer->Unpin(4);
  lru_replacer->Unpin(1);
  lru_replacer->Unpin(3);
  lru_replacer->Unpin(4);
  lru_replacer->Unpin(10);
  lru_replacer->Pin(3);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(4, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(1, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(10, result);
  EXPECT_EQ(0, lru_replacer->Evict(&result));

  lru_replacer->Unpin(5);
  lru_replacer->Unpin(6);
  lru_replacer->Unpin(7);
  lru_replacer->Unpin(8);
  lru_replacer->Unpin(6);
  lru_replacer->Pin(7);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(5, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(6, result);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(8, result);
  EXPECT_EQ(false, lru_replacer->Evict(&result));
  lru_replacer->Unpin(10);
  lru_replacer->Unpin(10);
  lru_replacer->Unpin(11);
  lru_replacer->Unpin(11);
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(10, result);
  lru_replacer->Pin(11);
  EXPECT_EQ(0, lru_replacer->Evict(&result));

  for (int i = 0; i <= 1000; i++) {
    lru_replacer->Unpin(i);
  }
  int j = 0;
  for (int i = 100; i < 1000; i += 2) {
    lru_replacer->Pin(i);
    EXPECT_EQ(true, lru_replacer->Evict(&result));
    if (j <= 99) {
      EXPECT_EQ(j, result);
      j++;
    } else {
      EXPECT_EQ(j + 1, result);
      j += 2;
    }
  }
  lru_replacer->Pin(result);

  delete lru_replacer;
}

TEST(LRUReplacerTest, Size) {
  auto lru_replacer = new LRUReplacer(10010);

  EXPECT_EQ(0, lru_replacer->Size());
  lru_replacer->Unpin(1);
  EXPECT_EQ(1, lru_replacer->Size());
  lru_replacer->Unpin(2);
  EXPECT_EQ(2, lru_replacer->Size());
  lru_replacer->Unpin(3);
  EXPECT_EQ(3, lru_replacer->Size());
  lru_replacer->Unpin(3);
  EXPECT_EQ(3, lru_replacer->Size());
  lru_replacer->Unpin(5);
  EXPECT_EQ(4, lru_replacer->Size());
  lru_replacer->Unpin(6);
  EXPECT_EQ(5, lru_replacer->Size());
  lru_replacer->Unpin(1);
  EXPECT_EQ(5, lru_replacer->Size());

  // pop element from replacer
  int result;
  for (int i = 5; i >= 1; i--) {
    lru_replacer->Evict(&result);
    EXPECT_EQ(i - 1, lru_replacer->Size());
  }
  EXPECT_EQ(0, lru_replacer->Size());

  for (int i = 0; i < 10000; i++) {
    lru_replacer->Unpin(i);
    EXPECT_EQ(i + 1, lru_replacer->Size());
  }
  for (int i = 0; i < 10000; i += 2) {
    lru_replacer->Pin(i);
    EXPECT_EQ(9999 - (i / 2), lru_replacer->Size());
  }

  delete lru_replacer;
}

TEST(LRUReplacerTest, ConcurrencyTest) {
  const int num_threads = 5;
  const int num_runs = 50;
  for (int run = 0; run < num_runs; run++) {
    int value_size = 1000;
    std::shared_ptr<LRUReplacer> lru_replacer{new LRUReplacer(value_size)};
    std::vector<std::thread> threads;
    int result;
    std::vector<int> value(value_size);
    for (int i = 0; i < value_size; i++) {
      value[i] = i;
    }
    auto rng = std::default_random_engine{};
    std::shuffle(value.begin(), value.end(), rng);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.push_back(std::thread([tid, &lru_replacer, &value]() {  // NOLINT
        int share = 1000 / 5;
        for (int i = 0; i < share; i++) {
          lru_replacer->Unpin(value[tid * share + i]);
        }
      }));
    }

    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }
    std::vector<int> out_values;
    for (int i = 0; i < value_size; i++) {
      EXPECT_EQ(1, lru_replacer->Evict(&result));
      out_values.push_back(result);
    }
    std::sort(value.begin(), value.end());
    std::sort(out_values.begin(), out_values.end());
    EXPECT_EQ(value, out_values);
    EXPECT_EQ(0, lru_replacer->Evict(&result));
  }
}

TEST(LRUReplacerTest, IntegratedTest) {
  int result;
  int value_size = 10000;
  auto lru_replacer = new LRUReplacer(value_size);
  std::vector<int> value(value_size);
  for (int i = 0; i < value_size; i++) {
    value[i] = i;
  }
  auto rng = std::default_random_engine{};
  std::shuffle(value.begin(), value.end(), rng);

  for (int i = 0; i < value_size; i++) {
    lru_replacer->Unpin(value[i]);
  }
  EXPECT_EQ(value_size, lru_replacer->Size());

  // Pin and unpin 777
  lru_replacer->Pin(777);
  lru_replacer->Unpin(777);
  // Pin and unpin 0
  EXPECT_EQ(1, lru_replacer->Evict(&result));
  EXPECT_EQ(value[0], result);
  lru_replacer->Unpin(value[0]);

  for (int i = 0; i < value_size / 2; i++) {
    if (value[i] != value[0] && value[i] != 777) {
      lru_replacer->Pin(value[i]);
      lru_replacer->Unpin(value[i]);
    }
  }

  std::vector<int> lru_array;
  for (int i = value_size / 2; i < value_size; ++i) {
    if (value[i] != value[0] && value[i] != 777) {
      lru_array.push_back(value[i]);
    }
  }
  lru_array.push_back(777);
  lru_array.push_back(value[0]);
  for (int i = 0; i < value_size / 2; ++i) {
    if (value[i] != value[0] && value[i] != 777) {
      lru_array.push_back(value[i]);
    }
  }
  EXPECT_EQ(value_size, lru_replacer->Size());

  for (int e : lru_array) {
    EXPECT_EQ(true, lru_replacer->Evict(&result));
    EXPECT_EQ(e, result);
  }
  EXPECT_EQ(value_size - lru_array.size(), lru_replacer->Size());

  delete lru_replacer;

}

}