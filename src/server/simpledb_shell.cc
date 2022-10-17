#ifndef SIMPLEDB_SHELL_CC
#define SIMPLEDB_SHELL_CC

#include "server/simpledb_shell.h"
#include "jdbc/embedded/embedded_driver.h"
#include "jdbc/embedded/embedded_metadata.h"
#include "parse/parser.h"

#include <unistd.h>
#include <iostream>

namespace SimpleDB {

void SimpleDB_shell_usage() {
    // std::cout << "- query: \n"
    //           << "          " << "select (field_name)\n"
    //           << "          " << "from (table_name)\n"
    //           << "          " << "[where (predicate)]\n" << std::endl;
    
    std::cout << "we can support simple query statement and modify statement" << std::endl;
}

bool SimpleDB_shell_verify_user(const std::string &username,
                                const std::string &password) {
    // not simplement
    return false;
}

EmbeddedConnection* SimpleDB_shell_init(const std::string &username,
                                        const std::string &password,
                                        const std::string &hostname) {
    std::unique_ptr<EmbeddedDriver> driver = std::make_unique<EmbeddedDriver>();
    
    auto connect_ = driver->connect(hostname, username, password);
    return connect_;
}

std::vector<std::string> SimpleDB_shell_parse_line(const std::string &line) {
    std::string tmp;
    std::vector<std::string> arr;
    int i = 0; // line index

    for (i = 0;i < static_cast<int>(line.size());i ++) {
        if (line[i] != ' ')
            break;
    }

    for (;i < static_cast<int>(line.size());i ++) {
        SIMPLEDB_ASSERT(line[i] != '\n', "");
        
        if (line[i] == ' ') {
            if (tmp.size()) {
                arr.push_back(tmp);
                tmp.clear();
            }
        }
        else {
            tmp += line[i];
        }
    }

    // don't emit this
    if (tmp.size()) {
        arr.push_back(tmp);
        tmp.clear();
    }

    return arr;
}

void SimpleDB_shell_eval(std::string &cmd, EmbeddedConnection *connect) {
    if (cmd.back() == '\n') {
        cmd.pop_back();
    }

    std::string buf = cmd;
    auto tokens = SimpleDB_shell_parse_line(buf);
    
    if (tokens.empty()) {
        // ignore this line
        return;
    }
    
    std::string first_token = tokens[0];
    auto statement = connect->createStatement();

    if (first_token == "select") {
        // query
        SimpleDB_shell_handle_query(statement, cmd);
    } else if (first_token == "update" ||
               first_token == "insert" ||
               first_token == "create" ||
               first_token == "delete") {
        // modify
        SimpleDB_shell_handle_update(statement, cmd);
    } else if (first_token == "quit") {
        connect->close();
        statement->close();
        delete statement;
        delete connect;
        exit(0);
    } 
    else {
        std::cerr << "syntax error" << std::endl;
    } 

    delete statement;
}

void SimpleDB_shell_handle_query(EmbeddedStatement *stmt, std::string &cmd) {
    EmbeddedResultSet* rs = stmt->executeQuery(sql::SQLString(cmd));
    EmbeddedMetaData* metadata = static_cast<EmbeddedMetaData*> (rs->getMetaData());
    int column_count = metadata->getColumnCount();
    
    for (int i = 1;i <= column_count;i ++) {
        // printf("%-10s", metadata->getColumnName(i).asStdString().c_str());
        std::cout << metadata->getColumnName(i).asStdString() << '\t' << std::flush;
    }
    std::cout << std::endl;
    
    while (rs->next()) {
        for (int i = 1;i <= column_count;i ++) {
            if (metadata->getColumnType(i) == FieldType::INTEGER) {
                // printf("%10d", rs->getInt(metadata->getColumnName(i).asStdString()));
                std::cout << rs->getInt(metadata->getColumnName(i).asStdString()) << "\t";
            } else {
                std::cout << rs->getString(metadata->getColumnName(i).asStdString()) << "\t";
            }
        }
        std::cout << std::endl; 
    }  

    rs->close();
    delete rs;
    delete metadata;
}

void SimpleDB_shell_handle_update(EmbeddedStatement *stmt, std::string &cmd) {
    auto rs = stmt->executeUpdate(sql::SQLString(cmd));
    if (rs != 0) {
        printf("update success\n");
        fflush(stdout);
    }
}

} // namespace SimpleDB
 
#endif