#ifndef DEBUG_FUNCTION_CC
#define DEBUG_FUNCTION_CC

#include <assert.h>
#include <iostream>

void Assert(bool flag, std::string s) {
	if(!flag) {
		std::cout << s << std::endl;
		assert(flag);
	}
}

template <typename T>
void PrintVector(const std::vector<T> &vec) {
    printf("size = %d, content = [", vec.size());
    for(int i = 4;i < vec.size();i ++) {
        if(i != vec.size() - 1)
            std::cout << vec[i] << ", ";
        else std::cout << vec[i];
    }
    std::cout << "]\n";
}

#endif
