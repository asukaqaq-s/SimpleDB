#ifndef EXCEPTION_H
#define EXCEPTION_H

#include <iostream>
#include "config/type.h"

namespace SimpleDB {

class Exception : public std::runtime_error {
public:
    explicit Exception(const std::string &message)
        : std::runtime_error(message) {
        std::string exception_message = "Message :: " + message + "\n";
        std::cerr << exception_message;
    }
};


class TransactionAbortException : public std::exception {
public:

    TransactionAbortException(txn_id_t txn_id, const std::string &reason)
        : txn_id_(txn_id), reason_(reason) {}

    const char* What() const  {
        return reason_.c_str();
    }

    txn_id_t txn_id_;
    std::string reason_;
};



class BadSyntaxException : public std::exception {
    
public:

    BadSyntaxException(const std::string &reason) 
        : reason_(reason) {}

    std::string reason_;
};



} // namespace SimpleDB


#endif