#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"
#include "record/table_iterator.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/operator_expression.h"
#include "parse/stream_tokenizer.h"
#include "parse/lexer.h"


#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>

namespace SimpleDB {
    
TEST(TokenizerTest, basicTest) {
    std::stringstream str;
    str << "table1.colA";
    
    Lexer l("table1.colA");
    std::cout << l.EatId() << std::endl;
    l.EatDelim('.');
    std::cout << l.EatId() << std::endl;
}


} // namespace SimpleDB
