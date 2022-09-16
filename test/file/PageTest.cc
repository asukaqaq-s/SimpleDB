#include "../../src/file/Page.cc"
#include "../../src/file/Page.h"
#include "../DebugFunction.cc"

#include <stdio.h>

int main() {

    SimpleDB::Page page(4096);

    // test SetInt and GetInt
    for(int i = 0;i < 1000;i ++) {
        page.SetInt(4 * i, i);
    }
    for(int i = 0;i < 1000;i ++) {
        int x = page.GetInt(4 * i);
        // printf("offset = %d, val = %d, x = %d\n", i * 4, i, x);
        Assert(x == i, "aborted when test GetInt"); // if x != i, abort 
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
        Assert(flag, "aborted when test GetBytes");
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
        Assert(s == test_s, "aborted when test GetString");
    }

    return 0;
}