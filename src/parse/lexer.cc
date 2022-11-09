#ifndef LEXER_CC
#define LEXER_CC

#include "parse/lexer.h"
#include "config/macro.h"
#include "config/exception.h"

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
    // allow '#' in comment
    tokenizer_->CommentChar('#');
    
    
    // move to the first token
    NextToken();
}


bool Lexer::MatchDelim(char d) {

    // the delimiter d will be setted to different characters
    return d == static_cast<char>(tokenizer_->TType_);
}


bool Lexer::MatchIntConstant() {
    return tokenizer_->TType_ == StreamTokenizer::TT_NUMBER &&
           CheckInteger(tokenizer_->NVal_);
}

bool Lexer::MatchRealConstant() {
    return tokenizer_->TType_ == StreamTokenizer::TT_NUMBER &&
           !CheckInteger(tokenizer_->NVal_);
}


bool Lexer::MatchStringConstant() {

    // a string constant format: 'aaa11313 21 2q'
    // so we just need to match two ' characters 

    // if we meet a string constant, ' is its TType
    // and sval_ is string constant.
    return '\'' == static_cast<char>(tokenizer_->TType_); 
}


bool Lexer::MatchKeyword(const std::string &key) {
    return tokenizer_->TType_ == StreamTokenizer::TT_WORD &&
           tokenizer_->SVal_ == key;
}



bool Lexer::MatchId() {
    return tokenizer_->TType_ == StreamTokenizer::TT_WORD &&
           (keywords_.find(tokenizer_->SVal_) == keywords_.end());
}


void Lexer::EatDelim(char d) {
    if (!MatchDelim(d)) {
        throw BadSyntaxException("eat delim error");
    }
    NextToken();
}


int Lexer::EatIntConstant() {
    if (!MatchIntConstant()) {
        throw BadSyntaxException("eat int error");
    }
    
    int res = static_cast<int>(tokenizer_->NVal_);
    NextToken();
    return res;
}


double Lexer::EatRealConstant() {
    if (!MatchRealConstant()) {
        throw BadSyntaxException("eat real error");
    }

    double res = tokenizer_->NVal_;
    NextToken();
    return res;
}


std::string Lexer::EatStringConstant() {
    if (!MatchStringConstant()) {
        throw BadSyntaxException("eat string error");
    }
    
    std::string res = tokenizer_->SVal_;
    NextToken();
    return res;
}

void Lexer::EatKeyword(const std::string &key) {
    if (!MatchKeyword(key)) {
        throw BadSyntaxException("eat keyword error");
    }

    NextToken();
}

std::string Lexer::EatId() {
    if (!MatchId()) {
        throw BadSyntaxException("eat id error");
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
       throw BadSyntaxException("next token error");
    }
}



void Lexer::InitKeywords() {

    keywords_ = std::set<std::string> {
        // basic keywords
        "select",  "from",   "where",  "and", "insert", "into",
        "values",  "delete", "update", "set", "create", "table",
        "varchar", "char", "int",    "view",   "as",  "index",  "on", "if", "not", "exist",
        // some other function
        "order", "by", "limit", "drop",
        // Aggregate functions
        "max", "min", "count", "avg", "sum"
    };


}


bool Lexer::CheckInteger(double val) {
    double ret = val - int(val);
    double eps = 1e-6;
    
    if (ret < eps) {
        return true;
    }
    return false;
}


} // namespace SimpleDB

#endif
