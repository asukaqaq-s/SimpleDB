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
#include "plans/update_plan.h"
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






TEST(UpdateParseTest, BasicTest) {
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
    auto colD = Column("cold", TypeID::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    cols.push_back(colD);
    auto schema1 = Schema(cols);

    // -------------------
    //  get table info
    // -------------------
    std::string test_file2 = "test2";
    mdm->CreateTable(test_file, schema1, txn.get());
    auto table_info = mdm->GetTable(test_file, txn.get());

    try
    {
        std::stringstream str;
        
        std::string s = "update test1 set "; 
        str << s;
        str << "cola = colc + 10, cold = 0.99";

        
        Parser p(str.str(), txn.get(), mdm.get());
        auto stmt = (p.ParseModify());
        auto update = static_cast<UpdateStatement*>(stmt.get());
        
        EXPECT_EQ(stmt->GetStmtType(), StatementType::UPDATE);
        EXPECT_EQ(update->table_name_, "test1");
        for (auto &t:update->set_) {
            std::cout << t->column_name_ << " " << t->expression_->ToString() << std::endl;
        }
        std::cout << std::endl;

    }
    catch(BadSyntaxException& e)
    {
        std::cerr << e.reason_ << '\n';
    }

}






} // namespace SimpleDB