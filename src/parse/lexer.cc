#ifndef LEXER_CC
#define LEXER_CC

#include "parse/lexer.h"
#include "config/macro.h"

#include <sstream>
#include <algorithm>

namespace SimpleDB {

Lexer::Lexer(const std::string &s) {
    InitKeywords();

    // a read/write stream on string    
    std::stringstream sstr(s);
    
    tokenizer_ = std::make_unique<StreamTokenizer> (sstr);
    // disallow '.' in identifiers
    tokenizer_->OrdinaryChar('.');
    // allow '_' in identifiers
    tokenizer_->WordChars('_', '_');
    // ids and keywords are converted to lower letter
    tokenizer_->LowerCaseMode(true);
    
    // move to the first token
    NextToken();
}

bool Lexer::MatchDelim(char d) {
    // the delimiter d will be setted to
    // different characters
    return d == static_cast<char>(tokenizer_->TType_);
}

bool Lexer::MatchIntConstant() {
    return tokenizer_->TType_ == StreamTokenizer::TT_NUMBER;
}

bool Lexer::MatchStringConstant() {
    // a string constant format
    // 'aaa11313 21 2q'
    // so we just need to match two ' characters 
    return '\'' == static_cast<char>(tokenizer_->TType_); 
}

bool Lexer::MatchKeyword(const std::string &key) {
    return tokenizer_->TType_ == StreamTokenizer::TT_WORD &&
           tokenizer_->SVal_ == key;
}

bool Lexer::MatchId() {
    return tokenizer_->TType_ == StreamTokenizer::TT_WORD &&
           !(keywords_.find(tokenizer_->SVal_) != keywords_.end());
}

void Lexer::EatDelim(char d) {
    if (!MatchDelim(d)) {
        SIMPLEDB_ASSERT(false, "bad syntax exception");
    }
    NextToken();
}

int Lexer::EatIntConstant() {
    if (!MatchIntConstant()) {
        SIMPLEDB_ASSERT(false, "bad syntax exception");
    }
    
    int res = static_cast<int>(tokenizer_->NVal_);
    NextToken();
    return res;
}

std::string Lexer::EatStringConstant() {
    if (!MatchStringConstant()) {
        SIMPLEDB_ASSERT(false, "bad syntax exception");
    }
    
    std::string res = tokenizer_->SVal_;
    NextToken();
    return res;
}

void Lexer::EatKeyword(const std::string &key) {
    if (!MatchKeyword(key)) {
        SIMPLEDB_ASSERT(false, "bad syntax exception");
    }

    NextToken();
}

std::string Lexer::EatId() {
    if (!MatchId()) {
        SIMPLEDB_ASSERT(false, "bad syntax exception");
    }

    std::string res = tokenizer_->SVal_;
    NextToken();
    return res;
}

void Lexer::NextToken() {
    try {
        tokenizer_->NextToken();
    }
    catch(std::exception &e) {
        SIMPLEDB_ASSERT(false, "bad syntax exception");
    }
}

void Lexer::InitKeywords() {
    keywords_ = std::set<std::string> {
        "select",  "from",   "where",  "and", "insert", "into",
        "values",  "delete", "update", "set", "create", "table",
        "varchar", "int",    "view",   "as",  "index",  "on"
    };
}


} // namespace SimpleDB

#endif
