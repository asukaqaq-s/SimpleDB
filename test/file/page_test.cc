#include "file/page.h"

#include "gtest/gtest.h"
#include <stdio.h>

namespace SimpleDB {

TEST(PageTest, simpletest1) {
    Page page(4096);

    // test SetInt and GetInt
    for(int i = 0;i < 1000;i ++) {
        page.SetInt(4 * i, i);
    }
    for(int i = 0;i < 1000;i ++) {
        int x = page.GetInt(4 * i);
        // printf("offset = %d, val = %d, x = %d\n", i * 4, i, x);
        EXPECT_EQ(x, i); // if x != i, abort 
    }
    
    // test SetBytes and GetBytes
    for(int i = 0;i < 500;i ++) {
        std::vector<char> ba{'1', '2', '3', '4'};
        page.SetBytes(8 * i,ba);
    }
    for(int i = 0;i < 500;i ++) {
        std::vector<char> test_ba{'1', '2', '3', '4'};
        std::vector<char> ba = page.GetBytes(8 * i);
        // PrintVector(test_ba);
        // PrintVector(ba);
        bool flag = (ba.size() == 4) && (ba == test_ba);
        EXPECT_EQ(flag, 1);
    }

    // test SetString and GetString
    for(int i = 0;i < 500;i ++) {
        std::string s;
        if(i % 2) {
            s = "4321"; 
        } else {
            s = "1234";
        }
        page.SetString(8 * i, s);
    }
    for(int i = 0;i < 500;i ++) {
        std::string test_s;
        std::string s;
        if(i % 2) {
            test_s = "4321"; 
        } else {
            test_s = "1234";
        }
        s = page.GetString(8 * i);
        EXPECT_EQ(s, test_s);
    }

    }
} // namespace SimpleDB 