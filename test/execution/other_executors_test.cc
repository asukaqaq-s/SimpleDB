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

TEST(InsertExecutorTest, BasicTest) {
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




    auto colA = Column("colA", TypeID::INTEGER);
    auto colB = Column("colB", TypeID::VARCHAR, 20);
    auto colC = Column("colC", TypeID::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);
    auto output_schema = Schema({colA, colB});

    auto valueA = Value((20010310));   
    auto valueB = Value("hello world", TypeID::VARCHAR);                    
    auto valueC = Value(3.14159);                          
    std::vector<Value> values{valueA, valueB, valueC};

    auto valueAA = Value((0504));   
    auto valueBB = Value("hello db", TypeID::VARCHAR);                    
    auto valueCC = Value(0.618);                          
    std::vector<Value> values2{valueAA, valueBB, valueCC};

    auto tuple = Tuple(values, schema);
    auto tuple_update = Tuple(values2, schema);
    
    
    std::vector<Tuple> tuple_list;
    int tuple_num = 1000;
    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        if (i % 2 == 0) {
            tuple_list.push_back(tuple);
        } else {
            tuple_list.push_back(tuple_update);
        }
    }

    mdm->CreateTable(test_file, schema, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());
    std::string table_name = table_info->table_name_;

    auto insert_plan = new InsertPlan(std::move(tuple_list), table_name);
    auto insert_executor = new InsertExecutor(&context, insert_plan, nullptr);
    insert_executor->Init();
    insert_executor->Next(nullptr);

    auto scan_plan = new SeqScanPlan(&output_schema, nullptr, table_name);
    auto scan_executor = new SeqScanExecutor(&context, scan_plan);
    scan_executor->Init();

    // auto result1 = tuple.KeyFromTuple(&schema, &output_schema);
    // auto result2 = tuple_update.KeyFromTuple(&schema, &output_schema);

    
    // int cnt = 0;
    // Tuple tmp;
    // while (scan_executor->Next(&tmp)) {
    //     if (cnt % 2 == 0) {
    //         EXPECT_EQ(result1, tmp);
    //     } else {
    //         EXPECT_EQ(result2, tmp);
    //     }
    //     cnt++;
    // }
    // EXPECT_EQ(cnt, tuple_num);

    // delete insert_plan;
    // delete insert_executor;
    // delete scan_plan;
    // delete scan_executor;
    // remove(filename.c_str());
}

TEST(InsertExecutorTest, BasicTest2) {
    // const std::string filename = "test.db";
    // const size_t buffer_pool_size = 3;
    // remove(filename.c_str());

    // auto disk_manager = new DiskManager(filename);
    // auto bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

    // auto colA = Column("colA", TypeID::INTEGER);
    // auto colB = Column("colB", TypeID::VARCHAR, 20);
    // auto colC = Column("colC", TypeID::DECIMAL);
    // std::vector<Column> cols;
    // cols.push_back(colA);
    // cols.push_back(colB);
    // cols.push_back(colC);
    // auto schema = Schema(cols);
    // auto output_schema = Schema({colA, colB});

    // auto valueA = Value(TypeID::INTEGER, static_cast<int64_t> (20010310));   
    // auto valueB = Value(TypeID::VARCHAR, "hello world");                    
    // auto valueC = Value(TypeID::DECIMAL, 3.14159);                          
    // std::vector<Value> values{valueA, valueB, valueC};

    // auto valueAA = Value(TypeID::INTEGER, static_cast<int64_t> (0504));   
    // auto valueBB = Value(TypeID::VARCHAR, "hello tinydb");                    
    // auto valueCC = Value(TypeID::DECIMAL, 0.618);                          
    // std::vector<Value> values2{valueAA, valueBB, valueCC};

    // auto tuple = Tuple(values, &schema);
    // auto tuple_update = Tuple(values2, &schema);
    // auto catalog = Catalog(bpm);
    // catalog.CreateTable("table", schema);
    // catalog.CreateTable("table2", schema);

    // std::vector<Tuple> tuple_list;
    // int tuple_num = 1000;
    // // insert many tuple
    // for (int i = 0; i < tuple_num; i++) {
    //     if (i % 2 == 0) {
    //         tuple_list.push_back(tuple);
    //     } else {
    //         tuple_list.push_back(tuple_update);
    //     }
    // }

    // ExecutionContext context(&catalog, bpm);

    // {
    //     // perform insertion
    //     auto insert_plan = new InsertPlan(std::move(tuple_list), table_name);
    //     auto insert_executor = new InsertExecutor(&context, insert_plan, nullptr);
    //     insert_executor->Init();
    //     insert_executor->Next(nullptr);
    //     delete insert_plan;
    //     delete insert_executor;
    // }

    // {
    //     // scan table and insert into table2
    //     auto scan_plan = new SeqScanPlan(&schema, nullptr, table_name);
    //     auto scan_executor = new SeqScanExecutor(&context, scan_plan);
    //     auto insert_plan = new InsertPlan(scan_plan, catalog.GetTable("table2")->oid_);
    //     auto insert_executor = new InsertExecutor(&context, insert_plan, std::unique_ptr<AbstractExecutor>(scan_executor));
    //     insert_executor->Init();
    //     insert_executor->Next(nullptr);

    //     // we've moved ownership of scan_executor to insert_executor
    //     delete scan_plan;
    //     delete insert_plan;
    //     delete insert_executor;
    // }

    // {
    //     // scan table2
    //     auto scan_plan = new SeqScanPlan(&output_schema, nullptr, catalog.GetTable("table2")->oid_);
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

    //     delete scan_plan;
    //     delete scan_executor;
    // }

    // delete bpm;
    // delete disk_manager;
    // remove(filename.c_str());
}


}