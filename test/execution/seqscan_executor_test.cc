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
#include "execution/executors/delete_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/update_executor.h"
#include "execution/execution_engine.h"




#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>

namespace SimpleDB {


int GetRandomInt() {
    int min = 0, max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    return distrib(engine);
}

double GetRandomDouble() {
    double min = 0, max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_real_distribution<> distrib(min, max); // uniform distribution
    return distrib(engine);
}

std::string GetRandomString() {
    int size = GetRandomInt() % 20;
    int min = 0, max = 9;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution

    std::string str;
    for (int i = 0;i < size; i++) {
        str += '0' + i % 10;
    }

    return str;
}


TEST(SeqScanTest, BasicTest) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
    auto txn = txn_mgr.Begin();

    std::unique_ptr<MetadataManager> mdm
        = std::make_unique<MetadataManager>(true, txn.get(), file_manager.get(), rm.get(), buf_manager.get());

    ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);

    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("colA", TypeID::INTEGER);
    auto colB = Column("colB", TypeID::VARCHAR, 20);
    auto colC = Column("colC", TypeID::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);
    auto output_schema = Schema( {colA, colB});

    // -------------------
    //  get table info
    // -------------------
    mdm->CreateTable(test_file, schema, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());


    // -------------------
    // create tuple 
    // -------------------
    auto valueA = Value((20010310));   
    auto valueB = Value("hello world", TypeID::VARCHAR);                    
    auto valueC = Value(3.14159);                          
    std::vector<Value> values{valueA, valueB, valueC};

    auto valueAA = Value((0504));   
    auto valueBB = Value("hello db", TypeID::VARCHAR);                    
    auto valueCC = Value(0.618);                          
    std::vector<Value> values2{valueAA, valueBB, valueCC};

    auto tuple1 = Tuple(values, schema);
    auto tuple2 = Tuple(values2, schema);



    // ---------------
    //  insert 
    // ---------------
    int tuple_cnt = 100;
    std::vector<RID> tuple1_rids;
    std::vector<RID> tuple2_rids;

    {
        auto *table_heap = table_info->table_heap_.get();
        for (int i = 0;i < tuple_cnt;i ++) {
            RID rid;
            if (i % 2 == 0) {
                table_heap->Insert(txn.get(), tuple1, &rid);
                tuple1_rids.emplace_back(rid);
            }
            else {
                table_heap->Insert(txn.get(), tuple2, &rid);
                tuple1_rids.emplace_back(rid);
            }
        }
    }
    

    // ----------------------------
    //  create a seqscan executor without predicate but filter some columns
    // ----------------------------
    {
        auto plan = new SeqScanPlan(&output_schema, nullptr, table_info->table_name_);
        auto executor = new SeqScanExecutor(&context, plan);
        executor->Init();

        Tuple tmp;
        int cnt = 0;
        while (executor->Next(&tmp)) {
            if (cnt % 2 == 0) {
                EXPECT_NE(tuple1, tmp);
            } else {
                EXPECT_NE(tuple2, tmp);
            }
            // rid should be valid, since other executor will depends on the RID
            // that is embedded in tuple. So we need to guarantee this property.
            EXPECT_EQ(tmp.GetRID().IsValid(), true);
            cnt++;
            // std::cout << tmp.ToString(schema) << std::endl;
        }
        EXPECT_EQ(cnt, tuple_cnt);

        delete plan;
        delete executor;
    }

    
    // ----------------------------
    //  create a seqscan executor with  predicate(F=C)
    // ----------------------------
    {
        auto column_expression = new ColumnValueExpression(TypeID::INTEGER, 0, "colA", &schema);
        auto constant_expression = new ConstantValueExpression(Value(20010310));
        auto predicate = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, 
                                                  column_expression, constant_expression);
        auto plan = new SeqScanPlan(&schema, predicate, table_info->table_name_);
        auto executor = new SeqScanExecutor(&context, plan);
        executor->Init();

        Tuple tmp;
        int cnt = 0;
        while (executor->Next(&tmp)) {
            EXPECT_EQ(tuple1, tmp);
            EXPECT_NE(tuple2, tmp);
            
            // rid should be valid, since other executor will depends on the RID
            // that is embedded in tuple. So we need to guarantee this property.
            EXPECT_EQ(tmp.GetRID().IsValid(), true);
            cnt++;
        }
        
        EXPECT_EQ(cnt, tuple_cnt/2);

        delete plan;
        delete executor;
        delete column_expression;
        delete constant_expression;
        delete predicate;
    }
    
    // ----------------------------
    //  create a seqscan executor with  predicate(C=F)
    // ----------------------------
    {
        auto column_expression = new ColumnValueExpression(TypeID::INTEGER, 0, "colA", &schema);
        auto constant_expression = new ConstantValueExpression(Value(20010310));
        auto predicate = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, 
                                                  constant_expression, column_expression);
        auto plan = new SeqScanPlan(&schema, predicate, table_info->table_name_);
        auto executor = new SeqScanExecutor(&context, plan);
        executor->Init();

        Tuple tmp;
        int cnt = 0;
        while (executor->Next(&tmp)) {
            EXPECT_EQ(tuple1, tmp);
            EXPECT_NE(tuple2, tmp);
            
            // rid should be valid, since other executor will depends on the RID
            // that is embedded in tuple. So we need to guarantee this property.
            EXPECT_EQ(tmp.GetRID().IsValid(), true);
            cnt++;
        }
        
        EXPECT_EQ(cnt, tuple_cnt/2);

        delete plan;
        delete executor;
        delete column_expression;
        delete constant_expression;
        delete predicate;
    }

    // ----------------------------
    //  create a seqscan executor with  predicate F1=F1
    // ----------------------------
    {
        auto column_expression = new ColumnValueExpression(TypeID::INTEGER, 0, "colA", &schema);
        auto predicate = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, 
                                                  column_expression, column_expression);
        auto plan = new SeqScanPlan(&schema, predicate, table_info->table_name_);
        auto executor = new SeqScanExecutor(&context, plan);
        executor->Init();

        Tuple tmp;
        int cnt = 0;
        while (executor->Next(&tmp)) {
            // rid should be valid, since other executor will depends on the RID
            // that is embedded in tuple. So we need to guarantee this property.
            EXPECT_EQ(tmp.GetRID().IsValid(), true);
            cnt++;
        }
        
        EXPECT_EQ(cnt, tuple_cnt);

        delete plan;
        delete executor;
        delete column_expression;
        delete predicate;
    }



    txn_mgr.Commit(txn.get());
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}



TEST(SeqScanTest, HardTest) {
    const std::string filename = "test.db";
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::unique_ptr<FileManager> file_manager 
        = std::make_unique<FileManager>(test_dir, 4096);
    
    std::unique_ptr<LogManager> log_manager 
        = std::make_unique<LogManager>(file_manager.get(), log_file_name);

    std::unique_ptr<RecoveryManager> rm 
        = std::make_unique<RecoveryManager>(log_manager.get());

    std::unique_ptr<BufferManager> buf_manager
        = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

    // --------------------
    //  create execution context
    // --------------------
    auto lock = std::make_unique<LockManager> ();
    TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
    auto txn = txn_mgr.Begin();

    std::unique_ptr<MetadataManager> mdm
        = std::make_unique<MetadataManager>(true, txn.get(), file_manager.get(), rm.get(), buf_manager.get());

    ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);

    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("colA", TypeID::INTEGER);
    auto colB = Column("colB", TypeID::VARCHAR, 20);
    auto colC = Column("colC", TypeID::DECIMAL);
    auto colD = Column("colD", TypeID::CHAR, 30);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema = Schema(cols);


    // -------------------
    //  get table info
    // -------------------
    mdm->CreateTable(test_file, schema, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());
    

    // ---------------
    //  insert 
    // ---------------
    int tuple_cnt = 1000;
    int equal_cnt = 0;
    {
        auto *table_heap = table_info->table_heap_.get();
        for (int i = 0;i < tuple_cnt;i ++) {
            RID rid;
            Tuple tuple;
            
            std::vector<Value> vec {
                Value(GetRandomInt()),
                Value(GetRandomString(), TypeID::VARCHAR),
                Value(GetRandomDouble()),
                Value(GetRandomString(), TypeID::CHAR)
            };
            tuple = Tuple(vec, schema);
            table_heap->Insert(txn.get(), tuple, nullptr);

            if (tuple.GetValue("colB", schema) == tuple.GetValue("colD", schema)) {
                equal_cnt ++;
            }
        }
    }
    
    
    // ---------------------------------------------------
    // | create a seqscan executor with  predicate F1=F2 |
    // ---------------------------------------------------
    {
        auto column_expression1 = new ColumnValueExpression(TypeID::VARCHAR, 0, "colB", &schema);
        auto column_expression2 = new ColumnValueExpression(TypeID::CHAR, 0, "colD", &schema);
        auto predicate = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, 
                                                  column_expression1, column_expression2);
        auto plan = new SeqScanPlan(&schema, predicate, table_info->table_name_);
        auto executor = new SeqScanExecutor(&context, plan);
        executor->Init();

        Tuple tmp;
        int cnt = 0;
        while (executor->Next(&tmp)) {
            // rid should be valid, since other executor will depends on the RID
            // that is embedded in tuple. So we need to guarantee this property.
            EXPECT_EQ(tmp.GetRID().IsValid(), true);
            cnt++;
        }
        std::cout << equal_cnt << std::endl;
        EXPECT_EQ(cnt, equal_cnt);

        delete plan;
        delete executor;
        delete column_expression1;
        delete column_expression2;
        delete predicate;
    }
    

    // -----------------------------------------------------------------
    // | create a seqscan executor with predicate is a expression tree |
    // -----------------------------------------------------------------
    // sql statement
    // select
    // where colA > 1 and colC <= 9.0 and colB = colD
    //
    // tree
    //                        And 
    //                       /   \ 
    //                      /     \ 
    //                     /       \ 
    //                    /         \ 
    //                  And          =
    //                  / \         /  \ 
    //                 /   \       /    \ 
    //                /     \    colB    colD
    //               /       \ 
    //          colA > 1  colC <= 9.0 
    //
    {
        // right child tree
        auto column_expression1 = new ColumnValueExpression(TypeID::VARCHAR, 0, "colB", &schema);
        auto column_expression2 = new ColumnValueExpression(TypeID::CHAR, 0, "colD", &schema);
        auto right_equal = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, 
                                                  column_expression1, column_expression2);
        

        // left child tree
        auto column_expression3 = new ColumnValueExpression(TypeID::INTEGER, 0, "colA", &schema);
        auto column_expression4 = new ColumnValueExpression(TypeID::DECIMAL, 0, "colC", &schema);
        auto constant_expression1 = new ConstantValueExpression(Value(1));
        auto constant_expression2 = new ConstantValueExpression(Value(9.0));

        auto left_ = new ComparisonExpression(ExpressionType::ComparisonExpression_GreaterThan, 
                                                   column_expression3, constant_expression1);
        auto right_ = new ComparisonExpression(ExpressionType::ComparisonExpression_LessThanEquals,
                                                    column_expression4, constant_expression2);
        auto left_and = new ConjunctionExpression(ExpressionType::ConjunctionExpression_AND, 
                                                  left_, right_);
        
        // root
        auto predicate = new ConjunctionExpression(ExpressionType::ConjunctionExpression_AND, 
                                                   left_and, right_equal);

        
        auto plan = new SeqScanPlan(&schema, predicate, table_info->table_name_);
        auto executor = new SeqScanExecutor(&context, plan);
        executor->Init();

        Tuple tmp;
        int cnt = 0;
        while (executor->Next(&tmp)) {
            // rid should be valid, since other executor will depends on the RID
            // that is embedded in tuple. So we need to guarantee this property.
            EXPECT_EQ(tmp.GetRID().IsValid(), true);
            cnt++;
            // std::cout << tmp.ToString(schema) << std::endl;
        }

        delete plan;
        delete executor;
        delete column_expression1;
        delete column_expression2;
        delete right_equal;
        delete column_expression3;
        delete column_expression4;
        delete constant_expression1;
        delete constant_expression2;
        delete left_;
        delete right_;
        delete left_and;
        delete predicate;
    }


    
    txn_mgr.Commit(txn.get());
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}








} // namespace SimpleDB