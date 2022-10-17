#include "server/simpledb.h"
#include "server/simpledb_shell.h"

#include <unistd.h>
#include <signal.h>
#include <cstring>
#include <gtest/gtest.h>

namespace SimpleDB {

#define MAXLINE     1024
#define MAXARGS     128

std::string PROMPT = "SimpleDB> ";
int verbose = 0;


void unix_error(const std::string &cmd);
typedef void handler_t(int);
// handler_t *Signal(int signum, handler_t *handler);


// int main (int argc, char **argv) {
TEST(MainTest, ServerTest) {
    // char c;
    char cmdline[MAXLINE];
    int emit_prompt = 1;
    
    
    // redirect stderr to stdout
    dup2(1, 2);
    
    // parse the command line
    // while ((c = getopt(argc, argv, "hvp")) != EOF) {
    //     switch (c) {
        
    //     case 'h':             /* print help message */
    //             SimpleDB_shell_usage();
	    
    //         break;

    //     case 'v':             /* emit additional diagnostic info */
    //             verbose = 1;
	    
    //         break;
    
    //     case 'p':             /* don't print a prompt */
    //             emit_prompt = 0;  /* handy for automatic testing */
	    
    //         break;
	
    
    //     default:
    //         SimpleDB_shell_usage();
	//     }
    // }

    // Signal(SIGINT, sigint_handler);
    // Signal(SIGQUIT, sigquit_handler);
    
    std::string username;
    std::string password;
    std::string hostname;
    
    std::cout << "please input username:" << std::endl;
    std::cin >> username;
    std::cout << "please input password:" << std::endl;
    std::cin >> password;
    std::cout << "please input hostname:" << std::endl;
    std::cin >> hostname;
    
    SimpleDB_shell_verify_user(username, password);
    auto connect_ = SimpleDB_shell_init(username, password, hostname);
    

    while (1) {
        
        if (emit_prompt) {
            std::cout << PROMPT << std::flush;
        }
        
        if ((fgets(cmdline, MAXLINE, stdin) == NULL ) &&
            ferror(stdin))
            unix_error("fgets error");
        // meet eof
        if (feof(stdin)) {
            std::cout << std::flush;
            exit(0);
        }
        std::string cmd(&cmdline[0], &cmdline[strlen(cmdline)]);
        // evaluate the command line
        SimpleDB_shell_eval(cmd, connect_);
        std::cout << std::flush;
    }

    delete connect_;
    // control never reaches here
    exit(0);
}



void unix_error(const std::string &cmd)
{
    std::cerr << cmd << std::endl;
    exit(1);
}

/*
 * Signal - wrapper for the sigaction function
 */
// handler_t *Signal(int signum, handler_t *handler) 
// {
//     struct sigaction action, old_action;

//     action.sa_handler = handler;  
//     sigemptyset(&action.sa_mask); /* block sigs of type being handled */
//     action.sa_flags = SA_RESTART; /* restart syscalls if possible */

//     if (sigaction(signum, &action, &old_action) < 0)
// 	unix_error("Signal error");
//     return (old_action.sa_handler);
// }

/**
* -------------------------------
* |        signal handler       |
* -------------------------------
*/
void sigquit_handler(int sig) 
{   
    std::cout << "Terminating after receipt of SIGQUIT signal" << std::endl;
    exit(1);
}


} // namespace SimpleDB