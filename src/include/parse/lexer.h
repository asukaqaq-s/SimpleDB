#ifndef LEXER_H
#define LEXER_H

#include "parse/stream_tokenizer.h"

#include <set>
#include <memory>

namespace SimpleDB {



/**
* @brief the lexical analyzer is used to analyze token legality
*/
class Lexer {

public:

    /**
    * @brief creates a new lexical analyzer for sql statement s
    * @param s the sql statement
    */
    Lexer(const std::string &s);

// methods to check the status of the current token

    /**
    * @brief return true if the token is the specified delimiter character.
    * 
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
    bool MatchDelim(char d);


    /**
    * @brief return true if the current token is an integer
    */
    bool MatchIntConstant();

    /**
    * @brief return true if the current token is and double
    */
    bool MatchRealConstant();


    /**
    * @brief return true if the current token is an string
    */
    bool MatchStringConstant();
    

    /**
    * @brief return true if the current token is the specified keyword.
    * 
    * @param key the keyword string
    */
    bool MatchKeyword(const std::string &key);


    /**
    * @brief return true if the current token is a legal identified.
    */
    bool MatchId();


// methods to "eat" the current token


    /**
    * @brief Throws an exception if the current token is not the specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
    void EatDelim(char d);


    /**
    * @brief throws an exception if the current token is not an integer.
    * otherwise, return that integer and moves to the next token.
    * @return the integer value of the current token
    */
    int EatIntConstant();


    /**
    * @brief throws an exception if the current token is not an double.
    * otherwise, return that double and moves to the next token.
    * @return the double value of the current token
    */
    double EatRealConstant();
    

    /**
    * @brief Throws an exception if the current token is not a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
    std::string EatStringConstant();



    /**
    * @brief an exception if the current token is not the specified keyword. 
    * Otherwise, moves to the next token.
    * @param key the keyword string
    */
    void EatKeyword(const std::string &key);



    /**
    * @brief Throws an exception if the current token is not an identifier. 
    * Otherwise, returns the identifier string and moves to the next token.
    * @return the string value of the current token
    */
    std::string EatId();


    int GetTType() { return tokenizer_->TType_; }

private:
    
 
    /**
    * @brief move to the next token
    */
    void NextToken();


    /**
    * @brief init keywords, we can simply add keywords by this method
    */
    void InitKeywords();


    /**
    * @brief check if integer
    */
    bool CheckInteger(double val);


private:


    std::set<std::string> keywords_;


    std::unique_ptr<StreamTokenizer> tokenizer_;

};




} // namespace SimpleDB


#endif
