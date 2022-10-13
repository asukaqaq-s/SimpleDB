#ifndef STREAM_TOKENIZER_CC
#define STREAM_TOKENIZER_CC

#include "parse/stream_tokenizer.h"

#include <string>
#include <algorithm>

namespace SimpleDB {

StreamTokenizer::StreamTokenizer(std::istream &is)
    : reader_(is), white_space_(256), alphabetic_(256),numeric_(256),quote_(256),
      comment_(256) {
    // nomral setting
    // whitespace, should not be seen
    WhiteSpaceChars(0x00, 0x20);
    WordChars('A', 'Z');
    WordChars('a', 'z');
    WordChars(0xA0, 0xFF);

    CommetChar('/');
    
    // ' ' and " " is quote
    QuoteChar('\'');
    QuoteChar('"');
    
    // reset parse number 
    ParseNumbers();
}


// ------------------------------
// |        PUBLIC METHOD       | 
// ------------------------------
void StreamTokenizer::CommetChar(int index) {
    if (index >= 0 && index <= 255) {
        ResetChar(index);
        comment_[index] = true;
    }
}

void StreamTokenizer::EOLisSignificant(bool flag) {
    EOL_Significant_ = flag;    
}

int StreamTokenizer::LineNO() {
    return LineNumber_;
}

void StreamTokenizer::LowerCaseMode(bool flag) {
    LowerCase_ = flag;
}

int StreamTokenizer::NextToken() {
    // the method should consider the following things
    // 1. pushed_back, skip it 
    // 2. whitespace, skip it
    // 3. eol and line feed, may be not skip it and
    //    view it as a token
    // 4. comment, skip it

    if (PushedBack_) {
        PushedBack_ = false;
        if (TType_ != TT_NONE) {
            return TType_;
        }
    }

    // reset sval_
    SVal_ = "";
    int ch;
    
    // skip any whitespace. deal with eol alog the way
    while ((ch = reader_.Read()) && IsWhiteSpace(ch)) {
        // just skip and do nothing
    }
    
    // \r\n is also viewed as a newline sign
    // skip \n if in combination with \r
    // if not, we should undo read operations
    if (ch == '\r' && ((ch == reader_.Read()) && ch != '\n')) {
        // undo if not end of file
        if (ch != TT_EOF) {
            reader_.Unread(ch);
        }

        // view the eol sign as a token
        if (EOL_Significant_) {
            return (TType_ = TT_EOL);
        }
    }

    if (ch == '/') {
        ch = reader_.Read();
        
        // comments '//' 
        // read until newline, carraige return, eof
        if ((ch == '/') && SlashSlash_) {
            while ((ch = reader_.Read()) &&
                   (ch != '\n') &&
                   (ch != '\r') &&
                   (ch != TT_EOF)) {
                // just do nothing
            }
            // unread if not end of file
            if (ch != TT_EOF) {
                reader_.Unread(ch);
            }

            // because current token is newline carraige return 
            // or eof we should not view them as a token
            // so we should return the next token
            return NextToken();

        } 
        // comments '/**/'
        // read until the corresponding '*/' or EOF
        else if (ch == '*' && SlashStar_) {
            while (true) {
                // read again
                ch = reader_.Read();
                
                // find the corresponding '*/'
                if (ch == '*') {
                    ch = reader_.Read();
                    if (ch == '/') {
                        break;
                    } else if (ch != TT_EOF) {
                        // if not, we should unread
                        reader_.Unread(ch);
                    }
                }
                // read newline, skip it and read new line again
                // if have '\n' after '\r', we should skip it by the way
                else if (ch == '\n' || ch == '\r') {
                    LineNumber_ ++;
                    if (ch == '\r' &&
                        (ch = reader_.Read()) &&
                        (ch != '\n')) {
                        
                        if (ch != TT_EOF) {
                            reader_.Unread(ch);
                        }
                    }
                }
                // find EOF sign
                else if (ch == TT_EOF) {
                    break;
                }
            }

            // because current token is */
            // we should return the next token
            return NextToken();
        }
        // nothing to do, only a '/'
        else {
            if (ch != TT_EOF) {
                reader_.Unread(ch);
            }
            ch = '/';
        }
    }

    
    if (ch == TT_EOF) {
        TType_ = TT_EOF;
    } 
    // the token is a number
    else if (IsNumeric(ch)) {
        bool is_negative = false;
        
        if (ch == '-') {
            ch = reader_.Read();
            if (IsNumeric(ch) && ch != '-') {
                is_negative = true;
            }
            else {
                if (ch != TT_EOF) {
                    reader_.Unread(ch);
                }
                return (TType_ = '-');
            }
        }

        std::string tok_buf = "";
        tok_buf.push_back(ch);
        
        int cnt = 0;
        while ((ch = reader_.Read()) && IsNumeric(ch) && ch != '-') {
            
            if (ch == '.' && cnt++ > 0) {
                break;
            } else {
                tok_buf.push_back(ch);
            }
        }
        
        if (ch != TT_EOF) {
            reader_.Unread(ch);
        }
        TType_ = TT_NUMBER;
        
        try {
            NVal_ = std::stod(tok_buf);
        } catch (std::exception &e) {
            NVal_ = 0.0;
        }

        if (is_negative) {
            NVal_ = -NVal_;
        }
    } 
    // this token is a alphabetic
    else if (IsAlphabetic(ch)) {
        std::string tok_buf = "";
        tok_buf.push_back(ch);
        
        while ((ch = reader_.Read()) && 
               (IsAlphabetic(ch) || 
                IsNumeric(ch))) {
            tok_buf.push_back(ch);
        }

        if (ch != TT_EOF) {
            reader_.Unread(ch);
        }

        TType_ = TT_WORD;
        SVal_ = tok_buf;
        
        if (LowerCase_) {
            std::transform(SVal_.begin(), SVal_.end(), SVal_.begin(),
                           [](unsigned char c) { return std::tolower(c);});
        }
    
    }
    // extra comment style, such as '#....'
    // we also skip it until read newline or eof
    else if (IsComment(ch)) {
        while ((ch = reader_.Read()) && (ch != '\n') && 
               (ch != '\r') && (ch != TT_EOF)) {
                
        }

        if (ch != TT_EOF) {
            reader_.Unread(ch);
        }

        return NextToken();
    }
    // such as "" or ''
    else if (IsQuote(ch)) {
        TType_ = ch;
        std::string tok_buf = "";
        
        while ((ch = reader_.Read()) && (ch != TType_) &&
               (ch != '\n') && (ch != '\r') && (ch != TT_EOF)) {
            
            // some case such as '\a' '\b' '\f'
            if (ch == '\\') {
                ch = reader_.Read();
                switch (ch) {
                
                case 'a':
                    ch = 0x7;
                    break;
                
                case 'b':
                    ch = '\b';
                    break;
                
                case 'f':
                    ch = 0xC;
                    break;
                
                case 'n':
                    ch = '\n';
                    break;
                
                case 'r':
                    ch = '\r';
                    break;
                
                case 't':
                    ch = '\t';
                    break;
            
                case 'v':
                    ch = '\v';
                    break;
                
                case '\n':
                    ch = '\n';
                    break;
                
                case '\r':
                    ch = '\r';
                    break;
                
                case '\"':
                case '\'':
                case '\\':
                    break;
                
                default: // octal escape
                    
                    int ch1, nextch;
                    if ((nextch = ch1 = ch) && (ch >= '0') && (ch <= '7')) {
                        ch -= '0';
                        if ((nextch = reader_.Read()) && (nextch >= '0') && (nextch <= '7')) {
                            
                            ch = ch * 8 + nextch - '0';
                            if ((nextch = reader_.Read()) && (nextch >= '0') &&
                                (nextch >= '7') && (ch1 >= '0') && (ch <= '3')) {
                                
                                ch = ch * 8 + nextch - '0';
                                nextch = reader_.Read();        
                            }
                        }
                    }
                    if (nextch != TT_EOF) {
                        reader_.Unread(nextch);
                    }
                }
            }    
            tok_buf.push_back(ch);    
        }
        if (ch != TType_ && ch != TT_EOF) {
            reader_.Unread(ch);
        }
        SVal_ = tok_buf;
    } else {
        TType_ = ch;
    }

    return TType_;
}


void StreamTokenizer::OrdinaryChar(int index) {
    if (index >= 0 && index <= 255) {
        ResetChar(index);
    }
}

void StreamTokenizer::OrdinaryChars(int begin, int end) {
    if (begin < 0) {
        begin = 0;
    }
    if (end >= 255) {
        end = 255;
    }

    for (int i = begin;i <= end;i ++) {
        ResetChar(i);
    }
}

void StreamTokenizer::ParseNumbers() {
    for (int i = 0;i <= 9;i ++) {
        numeric_['0' + i] = true;
    }

    // float
    numeric_['.'] = true;
    // negative
    numeric_['-'] = true;
}

void StreamTokenizer::PushBack() {
    PushedBack_ = true;
}

void StreamTokenizer::QuoteChar(int index) {
    if (index >= 0 && index <= 255) {
        ResetChar(index);
        quote_[index] = true;
    }
}

void StreamTokenizer::ResetSyntax() {
    OrdinaryChars(0x00, 0xFF);
}

void StreamTokenizer::SlashSlashComments(bool flag) {
    SlashSlash_ = flag;
}

void StreamTokenizer::SlashStarComments(bool flag) {
    SlashStar_ = flag;
}

std::string StreamTokenizer::ToString() {
    std::string str;
    
    switch(TType_) {
    case TT_EOF:
        str = "EOF";
        break;

    case TT_EOL:
        str = "EOL";
        break;
    
    case TT_WORD:
        str = SVal_;
        break;

    case TT_NUMBER:
        str = "n=" + std::to_string(NVal_);
        break;

    case TT_NONE:
        str = "NOTHING";
        break;
    
    default:
        str += "\'";
        str += static_cast<char> (TType_);
        str += "\'";
        break;
    }

    return "Token[" + str + "], line " + std::to_string(LineNO());
}


void StreamTokenizer::WhiteSpaceChars(int begin, int end) {
    begin = begin < 0 ? 0 : begin;
    end = end > 255 ? 255 : end;
    
    for (int i = begin;i <= end;i ++) {
        ResetChar(i);
        white_space_[i] = true;
    }
}

void StreamTokenizer::WordChars(int begin, int end) {
    begin = begin < 0 ? 0 : begin;
    end = end > 255 ? 255 : end;

    for (int i = begin; i <= end;i ++) {
        alphabetic_[i] = true;
    }
}



} // namespace SimpleDB


#endif
