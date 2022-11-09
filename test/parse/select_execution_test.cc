#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"
#include "record/table_iterator.h"
#include "metadata/metadata_manager.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/conjuction_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/operator_expression.h"
#include "parse/stream_tokenizer.h"
#include "parse/lexer.h"
#include "parse/parser.h"
#include "config/exception.h"


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



TEST(SelectExecutionTest, OneTableTest) {
     char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1";
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

    
    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("cola", TypeID::INTEGER);
    auto colB = Column("colb", TypeID::VARCHAR, 20);
    auto colC = Column("colc", TypeID::INTEGER);
    auto colD = Column("cold", TypeID::INTEGER);
    auto colE = Column("cole", TypeID::VARCHAR, 30);
    auto colF = Column("colf", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    cols.push_back(colE);
    cols.push_back(colF);
    auto schema1 = Schema(cols);




    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());

    // --------------------
    //  insert tuple
    // --------------------

    //
    // and or test, note that the priority of AND is greater than OR
    // so we can get the following expression tree 
    //  
    //              or
    //             / \ 
    //            /   \ 
    //           /     \ 
    //          =       and
    //         / \      / \ 
    //        /   \    /   \ 
    //       +    100 <=    =
    //      / \       / \ colb '0123'
    //     /   \     /   \ 
    //   colA  colc cold 90
    {
        int request_cnt = 0;
        std::function<Tuple (Schema schema)> GetRandomTuple = [&](Schema schema){
            std::vector<Value> value_list;
            for (auto t:schema.GetColumns()) {
                if (t.GetType() == TypeID::INTEGER) {
                    value_list.emplace_back(GetRandomInt());
                }
                else if (t.GetType() == TypeID::DECIMAL) {
                    value_list.emplace_back(GetRandomDouble());
                }
                else {
                    value_list.emplace_back(Value(GetRandomString(), TypeID::VARCHAR));
                }
            }
            if ((value_list[0] + value_list[2] == 100) || 
                (value_list[3] <= 90  && (value_list[1] == Value("0123", TypeID::VARCHAR)) )) {
                request_cnt++;
            }
            return Tuple(value_list, schema);
        };

        for (int i = 0;i < 1000;i ++) {
            table_info->table_heap_->Insert(txn.get(), GetRandomTuple(schema1), nullptr);
        }

        ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);
        std::string s = "select colA, colb from test1 where colA + colc = 100 or cold <= 90 and colb = '0123'";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        auto seqscannode = std::make_unique<SeqScanPlan>(std::make_shared<Schema>(schema1), select->where_.get(), test_file);
        auto seqscan = std::make_unique<SeqScanExecutor>(&context, seqscannode.get());
        

        Tuple tuple;
        seqscan->Init();
        int cnt = 0;
        while (seqscan->Next(&tuple)) {
            cnt ++;

        } 
        
        EXPECT_EQ(cnt, request_cnt);
    }

}


TEST(SelectExecutionTest, OneTableTest2) {
     char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1";
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

    
    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("cola", TypeID::INTEGER);
    auto colB = Column("colb", TypeID::VARCHAR, 20);
    auto colC = Column("colc", TypeID::INTEGER);
    auto colD = Column("cold", TypeID::INTEGER);
    auto colE = Column("cole", TypeID::VARCHAR, 30);
    auto colF = Column("colf", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    cols.push_back(colE);
    cols.push_back(colF);
    auto schema1 = Schema(cols);




    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());

    // --------------------
    //  insert tuple
    // --------------------

    //
    // and or test, note that the priority of AND is greater than OR
    // so we can get the following expression tree 
    //  
    //              or
    //             / \ 
    //            /   \ 
    //           /     \ 
    //          =       and
    //         / \      / \ 
    //        /   \    /   \ 
    //       +    100 <=    =
    //      / \       / \ colb '0123'
    //     /   \     /   \ 
    //   colA  colc cold 90
    {
        int request_cnt = 0;
        std::function<Tuple (Schema schema)> GetRandomTuple = [&](Schema schema){
            std::vector<Value> value_list;
            for (auto t:schema.GetColumns()) {
                if (t.GetType() == TypeID::INTEGER) {
                    value_list.emplace_back(GetRandomInt());
                }
                else if (t.GetType() == TypeID::DECIMAL) {
                    value_list.emplace_back(GetRandomDouble());
                }
                else {
                    value_list.emplace_back(Value(GetRandomString(), TypeID::VARCHAR));
                }
            }
            if ((value_list[0] + value_list[2] == 100) && 
                value_list[3] <= 90  || (value_list[1] == Value("0123", TypeID::VARCHAR)) ) {
                request_cnt++;
            }
            return Tuple(value_list, schema);
        };

        for (int i = 0;i < 1000;i ++) {
            table_info->table_heap_->Insert(txn.get(), GetRandomTuple(schema1), nullptr);
        }

        ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);
        std::string s = "select colA, colb from test1 where colA + colc = 100 and cold <= 90 or colb = '0123'";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        auto seqscannode = std::make_unique<SeqScanPlan>(std::make_shared<Schema>(schema1), select->where_.get(), test_file);
        auto seqscan = std::make_unique<SeqScanExecutor>(&context, seqscannode.get());
        

        Tuple tuple;
        seqscan->Init();
        int cnt = 0;
        while (seqscan->Next(&tuple)) {
            cnt ++;
        } 
        
        EXPECT_EQ(cnt, request_cnt);
    }

}


TEST(SelectExecutionTest, OneTableTest3) {
     char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1";
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

    
    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("cola", TypeID::INTEGER);
    auto colB = Column("colb", TypeID::VARCHAR, 20);
    auto colC = Column("colc", TypeID::INTEGER);
    auto colD = Column("cold", TypeID::INTEGER);
    auto colE = Column("cole", TypeID::VARCHAR, 30);
    auto colF = Column("colf", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    cols.push_back(colE);
    cols.push_back(colF);
    auto schema1 = Schema(cols);




    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());

    // --------------------
    //  insert tuple
    // --------------------

    //
    // and or test, note that the priority of AND is greater than OR
    // so we can get the following expression tree 
    //  
    //              or
    //             / \ 
    //            /   \ 
    //           /     \ 
    //          =       and
    //         / \      / \ 
    //        /   \    /   \ 
    //       +    100 <=    =
    //      / \       / \ colb '0123'
    //     /   \     /   \ 
    //   colA  colc cold 90
    {
        int request_cnt = 0;
        std::function<Tuple (Schema schema)> GetRandomTuple = [&](Schema schema){
            std::vector<Value> value_list;
            for (auto t:schema.GetColumns()) {
                if (t.GetType() == TypeID::INTEGER) {
                    value_list.emplace_back(GetRandomInt());
                }
                else if (t.GetType() == TypeID::DECIMAL) {
                    value_list.emplace_back(GetRandomDouble());
                }
                else {
                    value_list.emplace_back(Value(GetRandomString(), TypeID::VARCHAR));
                }
            }
            if ((value_list[0] + value_list[2] == 100) && 
                value_list[3] <= 90  || (value_list[1] == Value("0123", TypeID::VARCHAR)) &&
                value_list[4] == Value("0123", TypeID::VARCHAR) || 
                value_list[5] >= 10 ) {
                request_cnt++;
            }
            return Tuple(value_list, schema);
        };

        for (int i = 0;i < 1000;i ++) {
            table_info->table_heap_->Insert(txn.get(), GetRandomTuple(schema1), nullptr);
        }

        ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);
        std::string s = "select colA, colb from test1 where colA + colc = 100 and cold <= 90 or colb = '0123' and cole = '0123' or colf >=10";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        auto seqscannode = std::make_unique<SeqScanPlan>(std::make_shared<Schema>(schema1), select->where_.get(), test_file);
        auto seqscan = std::make_unique<SeqScanExecutor>(&context, seqscannode.get());
        

        Tuple tuple;
        seqscan->Init();
        int cnt = 0;
        while (seqscan->Next(&tuple)) {
            cnt ++;
        } 
        
        EXPECT_EQ(cnt, request_cnt);
    }

}



TEST(SelectExecutionTest, OneTableTest4) {
     char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1";
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

    
    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("cola", TypeID::INTEGER);
    auto colB = Column("colb", TypeID::VARCHAR, 20);
    auto colC = Column("colc", TypeID::INTEGER);
    auto colD = Column("cold", TypeID::INTEGER);
    auto colE = Column("cole", TypeID::VARCHAR, 30);
    auto colF = Column("colf", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    cols.push_back(colE);
    cols.push_back(colF);
    auto schema1 = Schema(cols);




    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());

    // --------------------
    //  insert tuple
    // --------------------

    {
        int request_cnt = 0;
        std::function<Tuple (Schema schema)> GetRandomTuple = [&](Schema schema){
            std::vector<Value> value_list;
            for (auto t:schema.GetColumns()) {
                if (t.GetType() == TypeID::INTEGER) {
                    value_list.emplace_back(GetRandomInt());
                }
                else if (t.GetType() == TypeID::DECIMAL) {
                    value_list.emplace_back(GetRandomDouble());
                }
                else {
                    value_list.emplace_back(Value(GetRandomString(), TypeID::VARCHAR));
                }
            }
            if (value_list[0] + value_list[2] <= 100 &&
                value_list[4] == Value("0123", TypeID::VARCHAR) &&
                value_list[5] >= 10 ) {
                request_cnt++;
            }
            return Tuple(value_list, schema);
        };

        for (int i = 0;i < 1000;i ++) {
            table_info->table_heap_->Insert(txn.get(), GetRandomTuple(schema1), nullptr);
        }

        ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);
        
        try {
            std::string s = "select colA, colb from test1 where  cola + colc <= 100 and cole = '0123' and colf >= 10";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            auto seqscannode = std::make_unique<SeqScanPlan>(std::make_shared<Schema>(schema1), select->where_.get(), test_file);
            auto seqscan = std::make_unique<SeqScanExecutor>(&context, seqscannode.get());
        

            Tuple tuple;
            seqscan->Init();
            int cnt = 0;
            while (seqscan->Next(&tuple)) {
                cnt ++;
            } 
            EXPECT_EQ(cnt, request_cnt);
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
        
        
    }

}




TEST(SelectExecutionTest, JoinTableTest) {
    
     char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1";
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

    
    // ----------------
    //  create schema
    // ----------------
    auto colA = Column("cola", TypeID::INTEGER);
    auto colB = Column("colb", TypeID::VARCHAR, 20);
    auto colC = Column("colc", TypeID::INTEGER);
    auto colD = Column("cold", TypeID::INTEGER);
    auto colE = Column("cole", TypeID::VARCHAR, 30);
    auto colF = Column("colf", TypeID::INTEGER);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);
    cols.clear();
    cols.push_back(colE);
    cols.push_back(colF);
    auto schema2 = Schema(cols);



    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    mdm->CreateTable(test_file2, schema2, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());
    auto table_info2 = mdm->GetTable(test_file2, txn.get());

    // --------------------
    //  insert tuple
    // --------------------
    
    //
    // and or test, note that the priority of AND is greater than OR
    // so we can get the following expression tree 
    //  
    //              or
    //             / \ 
    //            /   \ 
    //           /     \ 
    //          =       and
    //         / \      / \ 
    //        /   \    /   \ 
    //       +    100 <=    =
    //      / \       / \ colb '0123'
    //     /   \     /   \ 
    //   colA  colc cold 90
    {
        int request_cnt = 0;
        std::function<Tuple (Schema schema)> GetRandomTuple = [&](Schema schema){
            std::vector<Value> value_list;
            for (auto t:schema.GetColumns()) {
                if (t.GetType() == TypeID::INTEGER) {
                    value_list.emplace_back(GetRandomInt());
                }
                else if (t.GetType() == TypeID::DECIMAL) {
                    value_list.emplace_back(GetRandomDouble());
                }
                else {
                    value_list.emplace_back(Value(GetRandomString(), TypeID::VARCHAR));
                }
            }
            return Tuple(value_list, schema);
        };

        int tuple1_cnt = 0;
        int tuple2_cnt = 0;

        // insert table1
        for (int i = 0;i < 100;i ++) {
            auto tuple = GetRandomTuple(schema1);
            if (tuple.GetValue("cola", schema1) + tuple.GetValue("colc", schema1) <= 100) {
                tuple1_cnt++;
            }
            // std::cout << tuple.ToString(schema1) << std::endl;
            table_info->table_heap_->Insert(txn.get(), tuple, nullptr);
        }


        // insert table2 
        for (int i = 0;i < 100;i ++) {
            auto tuple = GetRandomTuple(schema2);
            if (tuple.GetValue("cole", schema2) == Value("0123", TypeID::VARCHAR)  && 
                tuple.GetValue("colf", schema2) >= 10) {
                tuple2_cnt++;
            }
            // std::cout << tuple.ToString(schema2) << std::endl;
            table_info2->table_heap_->Insert(txn.get(), tuple, nullptr);
        }




        ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);
        std::string s = "select colA, colb from test1, test2 where colA + colc <= 100 and cole = '0123' and colf >=10";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        auto seqscannode_left = std::make_unique<SeqScanPlan>(std::make_shared<Schema>(schema1), nullptr, test_file);
        auto seqscan_left = std::make_unique<SeqScanExecutor>(&context, seqscannode_left.get());
        auto seqscannode_right = std::make_unique<SeqScanPlan>(std::make_shared<Schema>(schema2), nullptr, test_file2);
        auto seqscan_right = std::make_unique<SeqScanExecutor>(&context, seqscannode_right.get());

        auto schema = Schema(schema1);
        schema.AddAllColumns(schema2);

        auto join_plan = std::make_unique<NestedLoopJoinPlan>(std::make_shared<Schema>(schema), 
                                                              seqscannode_left.get(), 
                                                              seqscannode_right.get(),
                                                              select->where_.get());
        auto join_executor = std::make_unique<NestedLoopJoinExecutor>(&context, join_plan.get(), 
                                                                      std::move(seqscan_left), 
                                                                      std::move(seqscan_right));

        
        Tuple tuple;
        join_executor->Init();
        int cnt = 0;
        while (join_executor->Next(&tuple)) {
            cnt ++;
            // std::cout << schema.ToString() << std::endl;
            // std::cout << tuple.ToString(schema) << std::endl;
        } 
        // std::cout << tuple1_cnt << " " << tuple2_cnt << std::endl;
        EXPECT_EQ(cnt, tuple1_cnt * tuple2_cnt);
    }

}



} // namespace SimpleDB