#ifndef SIMPLEDB_SHELL_H
#define SIMPLEDB_SHELL_H

#include "jdbc/embedded/embedded_connect.h"

#include <vector>
#include <string>

/**
* @brief 
*/

namespace SimpleDB {

void SimpleDB_shell_usage();

bool SimpleDB_shell_verify_user(const std::string &username,
                                const std::string &password);

EmbeddedConnection* SimpleDB_shell_init(const std::string &username,
                                        const std::string &password,
                                        const std::string &hostname);

std::vector<std::string> SimpleDB_shell_parse_line(const std::string &line);

void SimpleDB_shell_eval(std::string &cmd, EmbeddedConnection *connect);

void SimpleDB_shell_handle_update(EmbeddedStatement *stmt, std::string &cmd);

void SimpleDB_shell_handle_query(EmbeddedStatement *stmt, std::string &cmd);


} // namespace SimpleDB

#endif
