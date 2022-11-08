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
// #include "execution/executors/delete_executor.h"
// #include "execution/executors/insert_executor.h"
// #include "execution/executors/nested_loop_join_executor.h"
// #include "execution/executors/seq_scan_executor.h"
// #include "execution/executors/update_executor.h"
// #include "execution/execution_engine.h"




// #include <iostream>
// #include <memory>
// #include <random>
// #include <string>
// #include <cstring>
// #include <algorithm>


// namespace SimpleDB {

// TEST(InsertExecutorTest, BasicTest) {
//     const std::string filename = "test.db";
//     char buf[100];
//     std::string local_path = getcwd(buf, 100);
//     std::string test_dir = local_path + "/" + "test_dir";
//     std::string test_file = "test1.txt";
//     std::string cmd;
//     cmd = "rm -rf " + test_dir;
//     system(cmd.c_str());

//     std::string log_file_name = "log.log";
//     std::string log_file_path = test_dir + "/" + log_file_name;
//     std::unique_ptr<FileManager> file_manager 
//         = std::make_unique<FileManager>(test_dir, 4096);
    
//     std::unique_ptr<LogManager> log_manager 
//         = std::make_unique<LogManager>(file_manager.get(), log_file_name);

//     std::unique_ptr<RecoveryManager> rm 
//         = std::make_unique<RecoveryManager>(log_manager.get());

//     std::unique_ptr<BufferManager> buf_manager
//         = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

//     // --------------------
//     //  create execution context
//     // --------------------
//     auto lock = std::make_unique<LockManager> ();
//     TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
//     auto txn = txn_mgr.Begin();

//     std::unique_ptr<MetadataManager> mdm
//         = std::make_unique<MetadataManager>(true, txn.get(), file_manager.get(), rm.get(), buf_manager.get());

//     ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);




//     auto colA = Column("colA", TypeID::INTEGER);
//     auto colB = Column("colB", TypeID::VARCHAR, 20);
//     auto colC = Column("colC", TypeID::DECIMAL);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     cols.push_back(colB);
//     cols.push_back(colC);
//     auto schema = Schema(cols);
//     auto output_schema = Schema({colA, colB});

//     auto valueA = Value((20010310));   
//     auto valueB = Value("hello world", TypeID::VARCHAR);                    
//     auto valueC = Value(3.14159);                          
//     std::vector<Value> values{valueA, valueB, valueC};

//     auto valueAA = Value((0504));   
//     auto valueBB = Value("hello db", TypeID::VARCHAR);                    
//     auto valueCC = Value(0.618);                          
//     std::vector<Value> values2{valueAA, valueBB, valueCC};

//     auto tuple = Tuple(values, schema);
//     auto tuple_update = Tuple(values2, schema);
    
    
//     std::vector<Tuple> tuple_list;
//     int tuple_num = 1000;
//     // insert many tuple
//     for (int i = 0; i < tuple_num; i++) {
//         if (i % 2 == 0) {
//             tuple_list.push_back(tuple);
//         } else {
//             tuple_list.push_back(tuple_update);
//         }
//     }

//     mdm->CreateTable(test_file, schema, txn.get());
//     auto table_info = mdm->GetTable(test_file, txn.get());
//     std::string table_name = table_info->table_name_;

//     auto insert_plan = new InsertPlan(std::move(tuple_list), table_name);
//     auto insert_executor = new InsertExecutor(&context, insert_plan, nullptr);
//     insert_executor->Init();
//     insert_executor->Next(nullptr);

//     auto scan_plan = new SeqScanPlan(&output_schema, nullptr, table_name);
//     auto scan_executor = new SeqScanExecutor(&context, scan_plan);
//     scan_executor->Init();

//     auto result1 = tuple.KeyFromTuple(&schema, &output_schema);
//     auto result2 = tuple_update.KeyFromTuple(&schema, &output_schema);

    
//     int cnt = 0;
//     Tuple tmp;
//     while (scan_executor->Next(&tmp)) {
//         if (cnt % 2 == 0) {
//             EXPECT_EQ(result1, tmp);
//         } else {
//             EXPECT_EQ(result2, tmp);
//         }
//         cnt++;
//     }
//     EXPECT_EQ(cnt, tuple_num);

//     delete insert_plan;
//     delete insert_executor;
//     delete scan_plan;
//     delete scan_executor;
//     remove(filename.c_str());
// }

// TEST(InsertExecutorTest, BasicTest2) {
//     const std::string filename = "test.db";
//     char buf[100];
//     std::string local_path = getcwd(buf, 100);
//     std::string test_dir = local_path + "/" + "test_dir";
//     std::string test_file = "test1.txt";
//     std::string cmd;
//     cmd = "rm -rf " + test_dir;
//     system(cmd.c_str());

//     std::string log_file_name = "log.log";
//     std::string log_file_path = test_dir + "/" + log_file_name;
//     std::unique_ptr<FileManager> file_manager 
//         = std::make_unique<FileManager>(test_dir, 4096);
    
//     std::unique_ptr<LogManager> log_manager 
//         = std::make_unique<LogManager>(file_manager.get(), log_file_name);

//     std::unique_ptr<RecoveryManager> rm 
//         = std::make_unique<RecoveryManager>(log_manager.get());

//     std::unique_ptr<BufferManager> buf_manager
//         = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

//     // --------------------
//     //  create execution context
//     // --------------------
//     auto lock = std::make_unique<LockManager> ();
//     TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
//     auto txn = txn_mgr.Begin();

//     std::unique_ptr<MetadataManager> mdm
//         = std::make_unique<MetadataManager>(true, txn.get(), file_manager.get(), rm.get(), buf_manager.get());

//     ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);

//     auto colA = Column("colA", TypeID::INTEGER);
//     auto colB = Column("colB", TypeID::VARCHAR, 20);
//     auto colC = Column("colC", TypeID::DECIMAL);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     cols.push_back(colB);
//     cols.push_back(colC);
//     auto schema = Schema(cols);
//     auto output_schema = Schema({colA, colB});

//     auto valueA = Value((20010310));   
//     auto valueB = Value("hello world", TypeID::VARCHAR);                    
//     auto valueC = Value(3.14159);                          
//     std::vector<Value> values{valueA, valueB, valueC};

//     auto valueAA = Value((0504));   
//     auto valueBB = Value("hello tinydb", TypeID::VARCHAR);                    
//     auto valueCC = Value(0.618);                          
//     std::vector<Value> values2{valueAA, valueBB, valueCC};

//     auto tuple = Tuple(values, schema);
//     auto tuple_update = Tuple(values2, schema);

//     mdm->CreateTable("table1", schema, txn.get());
//     mdm->CreateTable("table2", schema, txn.get());

//     std::vector<Tuple> tuple_list;
//     int tuple_num = 1000;
//     // insert many tuple
//     for (int i = 0; i < tuple_num; i++) {
//         if (i % 2 == 0) {
//             tuple_list.push_back(tuple);
//         } else {
//             tuple_list.push_back(tuple_update);
//         }
//     }


//     {
//         // perform insertion
//         auto insert_plan = new InsertPlan(std::move(tuple_list), "table1");
//         auto insert_executor = new InsertExecutor(&context, insert_plan, nullptr);
//         insert_executor->Init();
//         insert_executor->Next(nullptr);
//         delete insert_plan;
//         delete insert_executor;
//     }


//     {
//         // scan table1 and insert into table2
//         auto scan_plan = new SeqScanPlan(&schema, nullptr, "table1");
//         auto scan_executor = new SeqScanExecutor(&context, scan_plan);
//         auto insert_plan = new InsertPlan(scan_plan, "table2");
//         auto insert_executor = new InsertExecutor(&context, insert_plan, 
//                                    std::unique_ptr<AbstractExecutor>(scan_executor));
//         insert_executor->Init();
//         insert_executor->Next(nullptr);

//         // we've moved ownership of scan_executor to insert_executor
//         delete scan_plan;
//         delete insert_plan;
//         delete insert_executor;
//     }

//     {
//         // scan table2
//         auto scan_plan = new SeqScanPlan(&output_schema, nullptr, "table2");
//         auto scan_executor = new SeqScanExecutor(&context, scan_plan);
//         scan_executor->Init();

//         auto result1 = tuple.KeyFromTuple(&schema, &output_schema);
//         auto result2 = tuple_update.KeyFromTuple(&schema, &output_schema);
//         int cnt = 0;
//         Tuple tmp;
//         while (scan_executor->Next(&tmp)) {
//             if (cnt % 2 == 0) {
//                 EXPECT_EQ(result1, tmp);
//             } else {
//                 EXPECT_EQ(result2, tmp);
//             }
//             cnt++;
//         }
//         EXPECT_EQ(cnt, tuple_num);

//         delete scan_plan;
//         delete scan_executor;
//     }
//     remove(filename.c_str());
// }


// TEST(UpdateExecutorTest, BasicTest) {
//     const std::string filename = "test.db";
//     char buf[100];
//     std::string local_path = getcwd(buf, 100);
//     std::string test_dir = local_path + "/" + "test_dir";
//     std::string test_file = "test1.txt";
//     std::string cmd;
//     cmd = "rm -rf " + test_dir;
//     system(cmd.c_str());

//     std::string log_file_name = "log.log";
//     std::string log_file_path = test_dir + "/" + log_file_name;
//     std::unique_ptr<FileManager> file_manager 
//         = std::make_unique<FileManager>(test_dir, 4096);
    
//     std::unique_ptr<LogManager> log_manager 
//         = std::make_unique<LogManager>(file_manager.get(), log_file_name);

//     std::unique_ptr<RecoveryManager> rm 
//         = std::make_unique<RecoveryManager>(log_manager.get());

//     std::unique_ptr<BufferManager> buf_manager
//         = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

//     // --------------------
//     //  create execution context
//     // --------------------
//     auto lock = std::make_unique<LockManager> ();
//     TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
//     auto txn = txn_mgr.Begin();

//     std::unique_ptr<MetadataManager> mdm
//         = std::make_unique<MetadataManager>(true, txn.get(), file_manager.get(), rm.get(), buf_manager.get());

//     ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);




//     auto colA = Column("colA", TypeID::INTEGER);
//     auto colB = Column("colB", TypeID::INTEGER);
//     auto colC = Column("colC", TypeID::DECIMAL);
//     std::vector<Column> cols;
//     cols.push_back(colA);
//     cols.push_back(colB);
//     cols.push_back(colC);
//     auto schema = Schema(cols);

//     auto valueA = Value((20010310));   
//     auto valueB = Value(10);
//     auto valueC = Value(3.14159);                          

//     auto tuple = Tuple({valueA, valueB, valueC}, schema);
//     mdm->CreateTable("table", schema, txn.get());
//     mdm->CreateTable("table2", schema, txn.get());
//     auto table_meta = mdm->GetTable("table", txn.get());

//     int tuple_num = 10;
//     // insert tuple
//     for (int i = 0; i < tuple_num; i++) {
//         RID rid;
//         table_meta->table_heap_->Insert(txn.get(), tuple, &rid);
//     }
    
//     // scenario: set colA = colA + 10, colB = 42
//     {
        
//         auto seq_plan = new SeqScanPlan(&schema, nullptr, "table");
//         auto seq_executor = new SeqScanExecutor(&context, seq_plan);
        
//         auto getColA = new ColumnValueExpression(TypeID::INTEGER, 0, "colA", &table_meta->schema_);
//         auto const10 = new ConstantValueExpression(Value(10));
//         auto add = new OperatorExpression(ExpressionType::OperatorExpression_Add, getColA, const10);

//         auto const42 = new ConstantValueExpression(Value(42));

//         auto update_plan = new UpdatePlan(seq_plan, "table", {{add, "colA"}, {const42, "colB"}});
//         auto update_executor = new UpdateExecutor(&context, update_plan, 
//                                    std::unique_ptr<AbstractExecutor>(seq_executor));
//         update_executor->Init();

//         int cnt = 0;
//         while (update_executor->Next(nullptr)) {
//             cnt++;
//         }
//         EXPECT_EQ(cnt, tuple_num);


//         delete seq_plan;
//         delete update_executor;
//         delete update_plan;
//         delete getColA;
//         delete const10;
//         delete add;
//         delete const42;
//     }

//     {
//         auto target_tuple = Tuple({20010320, 42, valueC}, schema);

//         auto seq_plan = new SeqScanPlan(&schema, nullptr, "table");
//         auto seq_executor = new SeqScanExecutor(&context, seq_plan);
//         seq_executor->Init();

//         int cnt = 0;
//         Tuple tuple;
//         while (seq_executor->Next(&tuple)) {
//             EXPECT_EQ(tuple, target_tuple);
//             cnt++;
//         }
//         EXPECT_EQ(cnt, tuple_num);

//         delete seq_plan;
//         delete seq_executor;
//     }


//     remove(filename.c_str());
// }

// TEST(LoopJoinExecutorTest, BasicTest) {
//     const std::string filename = "test.db";
//     char buf[100];
//     std::string local_path = getcwd(buf, 100);
//     std::string test_dir = local_path + "/" + "test_dir";
//     std::string test_file = "test1.txt";
//     std::string cmd;
//     cmd = "rm -rf " + test_dir;
//     system(cmd.c_str());

//     std::string log_file_name = "log.log";
//     std::string log_file_path = test_dir + "/" + log_file_name;
//     std::unique_ptr<FileManager> file_manager 
//         = std::make_unique<FileManager>(test_dir, 4096);
    
//     std::unique_ptr<LogManager> log_manager 
//         = std::make_unique<LogManager>(file_manager.get(), log_file_name);

//     std::unique_ptr<RecoveryManager> rm 
//         = std::make_unique<RecoveryManager>(log_manager.get());

//     std::unique_ptr<BufferManager> buf_manager
//         = std::make_unique<BufferManager>(file_manager.get(), rm.get(), 100);

//     // --------------------
//     //  create execution context
//     // --------------------
//     auto lock = std::make_unique<LockManager> ();
//     TransactionManager txn_mgr(std::move(lock), rm.get(), file_manager.get(), buf_manager.get());
//     auto txn = txn_mgr.Begin();

//     std::unique_ptr<MetadataManager> mdm
//         = std::make_unique<MetadataManager>(true, txn.get(), file_manager.get(), rm.get(), buf_manager.get());

//     ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);

    
//     {
//         auto colA = Column("colA", TypeID::INTEGER);
//         auto colB = Column("colB", TypeID::INTEGER);
//         auto colC = Column("colC", TypeID::DECIMAL);
//         auto schema = Schema({colA, colB, colC});
//         mdm->CreateTable("table1", schema, txn.get());
//         auto table_meta = mdm->GetTable("table1", txn.get());

//         // insert 5 tuple
//         // sheep: i'm not sure how to write styled code in such cases.
//         RID tmp;
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(2001311),
//                     Value(1),
//                     Value(3.14)
//                 },
//                 schema), &tmp);
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(2001312),
//                     Value(2),
//                     Value(3.14),
//                 },
//                 schema), &tmp);
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(2001313),
//                     Value(3),
//                     Value(3.14)
//                 },
//                 schema), &tmp);
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(2001314),
//                     Value(4),
//                     Value(3.14)
//                 },
//                 schema), &tmp);
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(2001315),
//                     Value(5),
//                     Value(3.14)
//                 },
//                 schema), &tmp);
//     }

//     {
//         auto colA = Column("colA", TypeID::INTEGER);
//         auto colB = Column("colB", TypeID::INTEGER);
//         auto schema = Schema({colA, colB});
//         mdm->CreateTable("table2", schema, txn.get());
//         auto table_meta = mdm->GetTable("table2", txn.get());


//         // insert 3 tuple
//         RID tmp;
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(2),
//                     Value(1),
//                 },
//                 schema), &tmp);
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(3),
//                     Value(-1),
//                 },
//                 schema), &tmp);
//         table_meta->table_heap_->Insert(
//                 txn.get(),
//                 Tuple(
//                 {   Value(6),
//                     Value(1),
//                 },
//                 schema), &tmp);
//     }



//     {
//         auto seq_plan_t1 = new SeqScanPlan(&mdm->GetTable("table1", txn.get())->schema_, nullptr, mdm->GetTable("table1", txn.get())->table_name_);
//         auto seq_plan_t2 = new SeqScanPlan(&mdm->GetTable("table2", txn.get())->schema_, nullptr, mdm->GetTable("table2", txn.get())->table_name_);

//         // expressions used to generate new tuple
//         auto getCol1 = new ColumnValueExpression(TypeID::INTEGER, 0, "colA", &mdm->GetTable("table1", txn.get())->schema_);
//         auto getCol2 = new ColumnValueExpression(TypeID::INTEGER, 0, "colB", &mdm->GetTable("table1", txn.get())->schema_);
//         auto getCol3 = new ColumnValueExpression(TypeID::INTEGER, 1, "colA", &mdm->GetTable("table2", txn.get())->schema_);
//         auto getCol4 = new ColumnValueExpression(TypeID::INTEGER, 1, "colB", &mdm->GetTable("table2", txn.get())->schema_);

//         // expression used to evaluate the legality
//         // since expression is stateless, so we can reuse the expression
//         auto comp = new ComparisonExpression(ExpressionType::ComparisonExpression_Equal, getCol2, getCol3);

//         auto colA = Column("colA", TypeID::INTEGER);
//         auto colB = Column("colB", TypeID::INTEGER);
//         auto colC = Column("colC", TypeID::INTEGER);
//         auto output_schema = Schema({colA, colB, colC});
//         auto join_plan = new NestedLoopJoinPlan(&output_schema, {seq_plan_t1, seq_plan_t2}, comp, {getCol1, getCol2, getCol4});

//         std::vector<Tuple> result;
//         ExecutionEngine engine;
//         engine.Execute(&context, join_plan, &result);

//         // result tuples
//         std::vector<Tuple> result_expected;
//         result_expected.push_back(
//             Tuple({ Value(2001312),
//                     Value(2),
//                     Value(1) }, output_schema));

//         result_expected.push_back(
//             Tuple({ Value(2001313),
//                     Value(3),
//                     Value(-1), }, output_schema));
        
//         EXPECT_EQ(result.size(), result_expected.size());
//         for (uint i = 0; i < result.size(); i++) {
//             EXPECT_EQ(result[i], result_expected[i]);
//             // LOG_DEBUG("expect: %s get: %s", result_expected[i].ToString(&output_schema).c_str(), 
//             //     result[i].ToString(&output_schema).c_str());
//         }

//         delete seq_plan_t1;
//         delete seq_plan_t2;
//         delete join_plan;
//         delete getCol1;
//         delete getCol2;
//         delete getCol3;
//         delete getCol4;
//         delete comp;
//     }


//     remove(filename.c_str());
// }



// }