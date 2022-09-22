#include <iostream>

int main() {
    int a = 0;
    int b = 0;
    std::cout << sizeof(a + sizeof(a)) << std::endl;

    return 0;
}