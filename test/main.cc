#include <unistd.h>
#include <string>
#include <sys/stat.h>

int main() {
    char buf[100];
    std::string directory(getcwd(buf, 100));
    // std::string cmd = "mkdir " + 
    std::string path = directory + "/resource";
    
    mkdir(path.c_str(),0777);
    path += "/*.temp";
    std::string cmd = "rm -rf " + path;
    system(cmd.c_str());
    return 0;
}