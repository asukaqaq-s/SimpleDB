#ifndef STREAM_TOKENIZER_H
#define STREAM_TOKENIZER_H

#include "parse/stream_reader.h"

namespace SimpleDB {

/**
* @brief this class can be viewed as a replacer
* of streamtokenizer in java.
* we can divide a string into serval tokens and 
* Identify whether a token is a numeric, letter 
* or a keyword by a streamtokenizer object
*/
class StreamTokenizer {

public:
    
    static constexpr int TT_EOF = -1;
    static constexpr int TT_EOL = '\n';
    static constexpr int TT_NUMBER = -2;
    static constexpr int TT_WORD = -3;
    static constexpr int TT_NONE = -4;

public:

    /** 
    * @brief create the object
    */
    StreamTokenizer(std::istream &is);

    /**
    * @brief set comment char which should be ignored
    */
    void CommetChar(int index);

    /**
    * @brief set whether eol can be viewed as a token
    */
    void EOLisSignificant(bool flag);
    
    /**
    * @brief return current line count
    */
    int LineNO();
    
    /**
    * @brief Set the lower_case mode
    * all token will be transformed to lower-letter
    */
    void LowerCaseMode(bool flag);
    
    /**
    * @brief move to the next token and return some val
    * 
    * @return TT_TYPE
    */
    int NextToken();

    /**
    * @brief clear
    */
    void OrdinaryChar(int index);
    
    /**
    * @brief clear
    */
    void OrdinaryChars(int begin, int end);
    
    void ParseNumbers();
    
    void PushBack();
    
    void QuoteChar(int index);
    
    void ResetSyntax();
    
    void SlashSlashComments(bool flag);
    
    void SlashStarComments(bool flag);
    

    std::string ToString();
    
    /**
    * @brief Set the letters in the range 
    * begin~end to be spaces
    */
    void WhiteSpaceChars(int begin, int end);

    /**
    * @brief Set the letters in the range
    * begin~end to be letters
    */
    void WordChars(int begin, int end);


private:

    inline bool IsWhiteSpace(int index) {
        return index >= 0 && index <= 255 && white_space_[index];
    }

    inline bool IsAlphabetic(int index) {
        return index >= 0 && index <= 255 && alphabetic_[index];
    }

    inline bool IsNumeric(int index) {
        return index >= 0 && index <= 255 && numeric_[index];
    }

    inline bool IsQuote(int index) {
        return index >= 0 && index <= 255 && quote_[index];
    }

    inline bool IsComment(int index) {
        return index >= 0 && index <= 255 && comment_[index];
    }

    inline void ResetChar(int index) {
        white_space_[index] = false;
        alphabetic_[index] = false;
        numeric_[index] = false;
        quote_[index] = false;
        comment_[index] = false;
    }

public: 

    int TType_ = TT_NONE;
    
    std::string SVal_;
    
    double NVal_;


private:

    // if eol is significant
    // we should set this value to true
    // and not skip any eol sign
    bool EOL_Significant_ = false;
    
    bool LowerCase_ = false;
    
    // this value means '//' is a comment style
    bool SlashSlash_ = false;
    
    // this value means '/**/' is a comment style
    bool SlashStar_ = false;
    
    bool PushedBack_ = false;
    
    int LineNumber_ = 1;

    // this object stores the input string
    StreamReader reader_;

    std::vector<bool> white_space_;
    
    std::vector<bool> alphabetic_;
    
    std::vector<bool> numeric_;
    
    std::vector<bool> quote_;
    
    std::vector<bool> comment_;

};

} // namespace SimpleDB



#endif