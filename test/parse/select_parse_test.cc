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




TEST(SelectParseTest, BasicTest) {
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
    auto colC = Column("colc", TypeID::DECIMAL);
    auto colD = Column("cold", TypeID::CHAR, 30);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);


    auto colE = Column("cole", TypeID::CHAR, 30);
    std::vector<Column> cols2;
    cols2.push_back(colA);
    cols2.push_back(colE);
    auto schema2 = Schema(cols2);


    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    mdm->CreateTable(test_file2, schema2, txn.get());

    // the simplest sql
    {
        std::string s = "select colA,colB from test1";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        
        EXPECT_EQ(select->select_list_[0].table_name_, "test1");
        EXPECT_EQ(select->select_list_[0].column_name_, "cola");
        EXPECT_EQ(select->select_list_[1].column_name_, "colb");

        EXPECT_EQ(select->tables_.size(), 1);
        EXPECT_EQ(*select->tables_.begin(), "test1");
    }

    // select a non-exist column
    {
        std::string s = "select colA,colE from test1";
        
        bool res = false;
        try {
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
        }
        catch (BadSyntaxException &t) {
            res = true;
        }
        EXPECT_TRUE(res);
    }


    // select two table but without overlap
    {
        std::string s = "select colB,cole from test1, test2";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();

        EXPECT_EQ(select->select_list_[0].table_name_, "test1");
        EXPECT_EQ(select->select_list_[0].column_name_, "colb");
        EXPECT_EQ(select->select_list_[1].table_name_, "test2");
        EXPECT_EQ(select->select_list_[1].column_name_, "cole");
        
        EXPECT_EQ(select->tables_.size(), 2);
        EXPECT_EQ(*select->tables_.begin(), "test1");
    }

    // select two table with overlap
    {
        bool res = false;
        try {
            std::string s = "select colA,cole from test1, test2";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
        }
        catch (BadSyntaxException &t) {
            res = true;
        }
        EXPECT_TRUE(res);
    }

}



TEST(SelectParseTest, WhereSimpleTest) {
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
    auto colC = Column("colc", TypeID::DECIMAL);
    auto colD = Column("cold", TypeID::CHAR, 30);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);


    auto colE = Column("cole", TypeID::CHAR, 30);
    std::vector<Column> cols2;
    cols2.push_back(colA);
    cols2.push_back(colE);
    auto schema2 = Schema(cols2);


    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    mdm->CreateTable(test_file2, schema2, txn.get());

    // F = C test
    {
        std::string s = "select colA,colB from test1 where colA = 10";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        
        EXPECT_EQ(select->where_->GetExpressionType(), ExpressionType::ComparisonExpression_Equal);
        EXPECT_EQ(select->where_->GetChildAt(0)->GetExpressionType(), 
                  ExpressionType::ColumnValueExpression);
        EXPECT_EQ(select->where_->GetChildAt(1)->GetExpressionType(), 
                  ExpressionType::ConstantValueExpression);
        EXPECT_EQ(select->where_->GetChildAt(1)->Evaluate(nullptr, nullptr), Value(10));

        EXPECT_EQ(select->where_->ToString(), "cola = 10");
    }

    // C = F test
    {
        std::string s = "select colA,colB from test1 where 10 = colA";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        
        EXPECT_EQ(select->where_->GetExpressionType(), ExpressionType::ComparisonExpression_Equal);
        EXPECT_EQ(select->where_->GetChildAt(1)->GetExpressionType(), 
                  ExpressionType::ColumnValueExpression);
        EXPECT_EQ(select->where_->GetChildAt(0)->GetExpressionType(), 
                  ExpressionType::ConstantValueExpression);
        EXPECT_EQ(select->where_->GetChildAt(0)->Evaluate(nullptr, nullptr), Value(10));

        EXPECT_EQ(select->where_->ToString(), "10 = cola");
    }


    // F1 != F2 test
    {
        std::string s = "select colA,colB from test1 where colB != colA";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        
        EXPECT_EQ(select->where_->GetExpressionType(), ExpressionType::ComparisonExpression_NotEqual);
        EXPECT_EQ(select->where_->GetChildAt(1)->GetExpressionType(), 
                  ExpressionType::ColumnValueExpression);
        EXPECT_EQ(select->where_->GetChildAt(0)->GetExpressionType(), 
                  ExpressionType::ColumnValueExpression);
        
        EXPECT_EQ(select->where_->ToString(), "colb != cola");
    }

    // F1 >= F2 test
    {
        std::string s = "select colA,colB from test1 where colB >= colA";
        Parser p(s, txn.get(), mdm.get());
        auto select = p.ParseSelect();
        
        EXPECT_EQ(select->where_->GetExpressionType(), ExpressionType::ComparisonExpression_GreaterThanEquals);
        EXPECT_EQ(select->where_->GetChildAt(1)->GetExpressionType(), 
                  ExpressionType::ColumnValueExpression);
        EXPECT_EQ(select->where_->GetChildAt(0)->GetExpressionType(), 
                  ExpressionType::ColumnValueExpression);
        
        EXPECT_EQ(select->where_->ToString(), "colb >= cola");
    }

    // column reference test
    {
        try {
            std::string s = "select test2.cola, colb from test1, test2 where cole >= test1.colA";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            EXPECT_EQ(select->where_->ToString(), "cole >= cola");
        } catch (BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }
    


}


// arithmetic test

TEST(SelectParseTest, WhereArithmeticTest) {
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
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);


    auto colE = Column("cole", TypeID::CHAR, 30);
    std::vector<Column> cols2;
    cols2.push_back(colA);
    cols2.push_back(colE);
    auto schema2 = Schema(cols2);


    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    mdm->CreateTable(test_file2, schema2, txn.get());

    // F1 = F2 + C test
    {
        try {
            std::string s = "select colA, colb from test1 where colA = 10 + colc";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            std::cout << select->where_->ToString() << std::endl;
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }

    // F1 + F2 = F3 * F1 test
    {
        try {
            std::string s = "select colA, colb from test1 where colA + colc = cold * cola";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            std::cout << select->where_->ToString() << std::endl;
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }


    // multipy table test
    {
        try {
            std::string s = "select test1.colA, colb from test1, test2 where test1.colA + colc = cold * test2.cola";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            std::cout << select->where_->ToString() << std::endl;
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }


}


TEST(SelectParseTest, AndOrTest) {
    
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
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);


    auto colE = Column("cole", TypeID::CHAR, 30);
    std::vector<Column> cols2;
    cols2.push_back(colA);
    cols2.push_back(colE);
    auto schema2 = Schema(cols2);


    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    mdm->CreateTable(test_file2, schema2, txn.get());

    // and and test
    {
        try {
            std::string s = 
"select cola, colb from test1 where cola = 10 + colc and cold + cola = colc and colc = 122";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            EXPECT_EQ(select->where_->ToString(), "cola = 10 + colc and cold + cola = colc and colc = 122");
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }

    // and or test
    {
        try {
            std::string s = 
"select cola, colb from test1 where cola = 10 + colc and cold + cola = colc or colc = 122";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            EXPECT_EQ(select->where_->ToString(), "cola = 10 + colc and cold + cola = colc or colc = 122");
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }

    // or and test
    {
        try {
            std::string s = 
"select cola, colb from test1 where cola = 10 + colc or cold + cola = colc and colc = 122";
            Parser p(s, txn.get(), mdm.get());
            auto select = p.ParseSelect();
            EXPECT_EQ(select->where_->ToString(), "cola = 10 + colc or cold + cola = colc and colc = 122");
        } catch(BadSyntaxException &e) {
            std::cout << e.reason_ << std::endl;
        }
    }
}


TEST(SelectParseTest, ExecutionTest) {
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
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);


    auto colE = Column("cole", TypeID::CHAR, 30);
    std::vector<Column> cols2;
    cols2.push_back(colA);
    cols2.push_back(colE);
    auto schema2 = Schema(cols2);


    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    mdm->CreateTable(test_file2, schema2, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());

    // --------------------
    //  insert tuple
    // --------------------
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

    for (int i = 0;i < 100;i ++) {
        table_info->table_heap_->Insert(txn.get(), GetRandomTuple(schema1), nullptr);
    }

    ExecutionContext context(mdm.get(), buf_manager.get(), txn.get(), &txn_mgr);
    std::string s = "select colA, colb from test1 where colA + colc = 10";
    Parser p(s, txn.get(), mdm.get());
    auto select = p.ParseSelect();
    auto seqscannode = std::make_unique<SeqScanPlan>(&schema1, select->where_.get(), test_file);
    auto seqscan = std::make_unique<SeqScanExecutor>(&context, seqscannode.get());
    

    Tuple tuple;
    seqscan->Init();
    while (seqscan->Next(&tuple)) {
       std::cout << tuple.ToString(schema1) << std::endl;
    } 

}



} // namespace SimpleDB