#include <iostream>

#include "parse/stream_tokenizer.h"
#include "gtest/gtest.h"

namespace SimpleDB {
void print_current_token(const std::set<std::string> &keywords,
                         StreamTokenizer &tok) {
    
    // std::cout << "type  = " << tok.TType_ << "  "
    //           << "Nval  = " << tok.NVal_ << " "
    //           << "Sval  = " << tok.SVal_ << std::endl; 

    if (tok.TType_ == StreamTokenizer::TT_NUMBER) {
        std::cout << "IntConstant " << tok.NVal_ << std::endl;
    } 
    
    else if (tok.TType_ == StreamTokenizer::TT_WORD) {
        std::string word = tok.SVal_;
        
        if (keywords.find(word) != keywords.end()) {
            std::cout << "Keyword " << word << std::endl;
        } else {
            std::cout << "Id " << word << std::endl;
        }
    } 
    
    else if (tok.TType_ == '\'') {
        std::cout << "StringConstant " << tok.SVal_ << std::endl;
    } 
    
    else {
        std::cout << "Delimiter " << static_cast<char>(tok.TType_) << std::endl;
    }
}

TEST(parse, tokenizer_test) {
  std::set<std::string> keywords = {
      "select", "from",    "where",  "and", "insert", "into",
      "values", "delete",  "update", "set", "create", "table",
      "int",    "varchar", "view",   "as",  "index",  "on"};

  std::string s;
  std::stringstream ss;
  ss << "select a from x, z where b = 3 and c = 'foobar'";

  StreamTokenizer tok(ss);
  tok.OrdinaryChar('.');
  tok.WordChars('_', '_');
  tok.LowerCaseMode(true);
  while (tok.NextToken() != StreamTokenizer::TT_EOF) {
    print_current_token(keywords, tok);
  }

}
} // namespace SimpleDB
