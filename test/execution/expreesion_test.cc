// #include "log/log_iterator.h"
// #include "log/log_manager.h"
// #include "recovery/log_record.h"
// #include "buffer/buffer_manager.h"
// #include "gtest/gtest.h"
// #include "recovery/recovery_manager.h"
// #include "record/table_heap.h"
// #include "record/table_iterator.h"
// #include "execution/expressions/column_value_expression.h"
// #include "execution/expressions/comparison_expression.h"
// #include "execution/expressions/conjuction_expression.h"
// #include "execution/expressions/conjuction_expression.h"
// #include "execution/expressions/conjuction_expression.h"
// #include "execution/expressions/constant_value_expression.h"
// #include "execution/expressions/operator_expression.h"



// #include <iostream>
// #include <memory>
// #include <random>
// #include <string>
// #include <cstring>
// #include <algorithm>




// namespace SimpleDB {


// TEST(ExpressionTest, ColumnTest) {
//     auto colA = Column("colA", TypeID::INTEGER);
//     auto colB = Column("colB", TypeID::INTEGER);
//     auto colC = Column("colC", TypeID::INTEGER);
//     auto colD = Column("colD", TypeID::DECIMAL);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     cols.push_back(colB);
//     cols.push_back(colC);
//     cols.push_back(colD);

//     auto schema = Schema(cols);

//     auto column_value_expression1 = std::make_shared<ColumnValueExpression> (TypeID::INTEGER, 0, "colA", &schema);
//     auto column_value_expression2 = std::make_shared<ColumnValueExpression> (TypeID::INTEGER, 1, "colB", &schema);

//     // generate tuple
//     Tuple tuple1, tuple2;
//     auto valueA1 = Value((1));
//     auto valueB1 = Value((20010310));
//     auto valueC1 = Value((42));
//     auto valueD1 = Value(3.14159);
//     tuple1 = Tuple({valueA1, valueB1, valueC1, valueD1}, schema);

//     auto valueA2 = Value((2));
//     auto valueB2 = Value((19700101));
//     auto valueC2 = Value((42));
//     auto valueD2 = Value(3.14159);
//     tuple2 = Tuple({valueA2, valueB2, valueC2, valueD2}, schema);

//     EXPECT_EQ(column_value_expression1->Evaluate(&tuple1, &tuple2), valueA1);
//     EXPECT_EQ(column_value_expression2->Evaluate(&tuple1, &tuple2), valueB2);

// }


// TEST(ExpressionTest, ComparisonExpressionTest) {
//     auto colA = Column("colA", TypeID::INTEGER);
//     auto colB = Column("colB", TypeID::INTEGER);
//     auto colC = Column("colC", TypeID::INTEGER);
//     auto colD = Column("colD", TypeID::DECIMAL);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     cols.push_back(colB);
//     cols.push_back(colC);
//     cols.push_back(colD);
//     auto schema_left = Schema(cols);

//     auto colA2 = Column("colA", TypeID::INTEGER);
//     auto colB2 = Column("colB", TypeID::DECIMAL);
//     auto schema_right = Schema({colA2, colB2});

//     auto column_value_expression1 = std::make_shared<ColumnValueExpression>(TypeID::INTEGER, 0, "colC", &schema_left);
//     auto column_value_expression2 = std::make_shared<ColumnValueExpression>(TypeID::INTEGER, 1, "colA", &schema_right);

//     // generate tuple
//     Tuple tuple1, tuple2, tuple3;
//     auto valueA1 = Value((1));
//     auto valueB1 = Value((20010310));
//     auto valueC1 = Value((24));
//     auto valueD1 = Value(3.14159);
//     tuple1 = Tuple({valueA1, valueB1, valueC1, valueD1}, schema_left);

//     auto valueA2 = Value((42));
//     auto valueB2 = Value(3.14159);
//     tuple2 = Tuple({valueA2, valueB2}, schema_right);

//     tuple3 = Tuple({valueA1, valueB1, valueA2, valueD1}, schema_left);

//     // should we wrap this? such as MakeGreaterThanExpression
//     // 1.ColC > 2.ColA
//     auto comparison_expression1 = std::make_shared<ComparisonExpression> (
//         ExpressionType::ComparisonExpression_GreaterThan, column_value_expression1, column_value_expression2);
    
//     // 1.ColC = 2.ColA
//     auto comparison_expression2 = std::make_shared<ComparisonExpression>(
//         ExpressionType::ComparisonExpression_Equal, column_value_expression1, column_value_expression2);

//     // 24 < 42, false
//     EXPECT_EQ(comparison_expression1->Evaluate(&tuple1, &tuple2).IsTrue(), false);
//     // 24 < 42, false
//     EXPECT_EQ(comparison_expression2->Evaluate(&tuple1, &tuple2).IsTrue(), false);
//     // 42 = 42, true
//     EXPECT_EQ(comparison_expression2->Evaluate(&tuple3, &tuple2).IsTrue(), true);

// }



// TEST(ExpressionTest, ConjunctionExspressionTest) {
//     auto true1 = std::make_shared<ConstantValueExpression>(Value(true));
//     auto false1 = std::make_shared<ConstantValueExpression>(Value(false));
//     auto true2 = std::make_shared<ConstantValueExpression>(Value(true));
//     auto false2 = std::make_shared<ConstantValueExpression>(Value(false));

//     auto conj_and_false = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_AND, true1, false1);
//     auto conj_or_true = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_OR, true1, false1);
//     auto conj_and_true = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_AND, true1, true2);
//     auto conj_or_false = std::make_unique<ConjunctionExpression>(ExpressionType::ConjunctionExpression_OR, false1, false2);

//     // true & false
//     EXPECT_TRUE(conj_and_false->Evaluate(nullptr, nullptr).IsFalse());
//     EXPECT_TRUE(conj_or_true->Evaluate(nullptr, nullptr).IsTrue());
//     EXPECT_TRUE(conj_and_true->Evaluate(nullptr, nullptr).IsTrue());
//     EXPECT_TRUE(conj_or_false->Evaluate(nullptr, nullptr).IsFalse());

// }

// TEST(OperatorExpressionTest, BasicTest) {
//     auto colA = Column("colA", TypeID::INTEGER);
//     auto colB = Column("colB", TypeID::INTEGER);
//     auto colC = Column("colC", TypeID::INTEGER);
//     auto colD = Column("colD", TypeID::DECIMAL);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     cols.push_back(colB);
//     cols.push_back(colC);
//     cols.push_back(colD);
//     auto schema_left = Schema(cols);

//     auto colA2 = Column("colA", TypeID::INTEGER);
//     auto colB2 = Column("colB", TypeID::DECIMAL);
//     auto schema_right = Schema({colA2, colB2});
    
//     auto column_value_expression1 = std::make_shared<ColumnValueExpression>(TypeID::INTEGER, 0, "colC", &schema_left);
//     auto column_value_expression2 = std::make_shared<ColumnValueExpression>(TypeID::INTEGER, 1, "colA", &schema_right);
//     auto column_value_expression3 = std::make_shared<ColumnValueExpression>(TypeID::INTEGER, 0, "colA", &schema_left);

//     // generate tuple
//     Tuple tuple1, tuple2, tuple3;
//     auto valueA1 = Value((1));
//     auto valueB1 = Value((20010310));
//     auto valueC1 = Value((24));
//     auto valueD1 = Value(3.14159);
//     tuple1 = Tuple({valueA1, valueB1, valueC1, valueD1}, schema_left);

//     auto valueA2 = Value((42));
//     auto valueB2 = Value(3.14159);
//     tuple2 = Tuple({valueA2, valueB2}, schema_right);

//     tuple3 = Tuple({valueA1, valueB1, valueA2, valueD1}, schema_left);

//     auto operator_expression1 = std::make_shared<OperatorExpression>(
//         ExpressionType::OperatorExpression_Add, column_value_expression1, column_value_expression2);
//     EXPECT_EQ(operator_expression1->GetReturnType(), TypeID::INTEGER);
//     EXPECT_EQ(operator_expression1->Evaluate(&tuple1, &tuple2), Value(66));

//     auto operator_expression2 = std::make_shared<OperatorExpression>(
//         ExpressionType::OperatorExpression_Subtract, column_value_expression3, column_value_expression2);
//     EXPECT_EQ(operator_expression2->GetReturnType(), TypeID::INTEGER);
//     EXPECT_EQ(operator_expression2->Evaluate(&tuple1, &tuple2), Value(-41));

// }



// } // namespace SimpleDB