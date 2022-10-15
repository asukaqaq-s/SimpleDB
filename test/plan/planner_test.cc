#include "plan/planner.h"
#include "gtest/gtest.h"
#include "metadata/metadata_manager.h"
#include "plan/basic_update_planner.h"
#include "plan/better_query_planner.h"
#include <random>

namespace SimpleDB {

TEST(PLANTEST, PLANTEST1) {
  int min = 0,max = 100;
    

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd2;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::string testFile = "testfile";
    std::string testFile1 = "testfile.table";

    cmd2 = "rm -rf " + test_dir;
    system(cmd2.c_str());

    FileManager fm(test_dir, 4096);
    
    LogManager lm(&fm, log_file_name);
    
    BufferManager bm(&fm, &lm, 10);

    std::string mytable = "MyTable";

    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));
    std::unique_ptr<Transaction> tx = std::make_unique<Transaction>(&fm, &lm, &bm); 

    std::unique_ptr<MetadataManager> mdm = std::make_unique<MetadataManager>(true, tx.get());
    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &lm, &bm); 
    std::unique_ptr<QueryPlanner> q1 = std::make_unique<BetterQueryPlanner>(mdm.get());
    std::unique_ptr<UpdatePlanner> q2 = std::make_unique<BasicUpdatePlanner>(mdm.get());

    Planner plnr(std::move(q1),std::move(q2));

    
    std::string cmd = "create table T1(A int, B varchar(9))";
    plnr.ExecuteUpdate(cmd, tx.get());
    // Layout layout = mdm->GetLayout("t1", tx.get());
    // std::cout <<"layout = " << std::endl;
    
    // for (auto s:layout.GetSchema().GetFields()) {
    //     std::cout << "field = " << s << std::endl;
    // }

    int n = 200;
    std::cout << "Inserting " << n << " random records" << std::endl;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> d(min, max); // uniform distribution
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());

    for (int i = 0; i < n; i++) {
        int a = d(engine);
        // int a = 10;
        std::string b = "rec" + std::to_string(a);
        cmd = "insert into T1(A,B) values (" + std::to_string(a) + ", '" + b + "')";
        plnr.ExecuteUpdate(cmd, tx.get());
    }
    
    // TableScan ts(tx.get(), "t1", layout);
    // ts.FirstTuple();
    // int nt1 = 0;
    // while (ts.Next()) {
    //     std::cout << "table scan doing " << nt1 ++ << std::endl;
    // }


    // cmd = "update T1 set A=10";
    // std::cout << "tuple numbers = " << plnr.ExecuteUpdate(cmd, tx.get()) << std::endl;

    std::cout << "-----------------------------" << std::endl;

    std::string qry = "select A,B from T1 where A=10";
    auto p = plnr.CreateQueryPlan(qry, tx.get());
    auto s = p->Open();
    

    s->FirstTuple();
    int cnt = 0;
    while (s->Next()) {
        cnt ++;
        std::cout << s->GetInt("a") << "   " << s->GetString("b") << std::endl;
    }
    s->Close();
    tx->Commit();
    std::cout << "cnt     =            " << cnt << std::endl;

    cmd2 = "rm -rf " + test_dir;
    system(cmd2.c_str());
}


TEST(PLANTEST, PLANTEST2) {
  int min = 0,max = 100;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> d(min, max); // uniform distribution
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd2;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::string testFile = "testfile";
    std::string testFile1 = "testfile.table";

    cmd2 = "rm -rf " + test_dir;
    system(cmd2.c_str());

    FileManager fm(test_dir, 4096);
    
    LogManager lm(&fm, log_file_name);
    
    BufferManager bm(&fm, &lm, 10);

    std::string mytable = "MyTable";

    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));
    std::unique_ptr<Transaction> tx = std::make_unique<Transaction>(&fm, &lm, &bm); 

    std::unique_ptr<MetadataManager> mdm = std::make_unique<MetadataManager>(true, tx.get());
    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &lm, &bm); 
    std::unique_ptr<QueryPlanner> q1 = std::make_unique<BetterQueryPlanner>(mdm.get());
    std::unique_ptr<UpdatePlanner> q2 = std::make_unique<BasicUpdatePlanner>(mdm.get());

    Planner plnr(std::move(q1),std::move(q2));

    
    std::string cmd = "create table T1(A int, B varchar(9))";
    plnr.ExecuteUpdate(cmd, tx.get());
    
    int n = 200;
    std::cout << "Inserting " << n << " records int T1." << std::endl;

    for (int i = 0; i < n; i++) {
        int a = i;
        std::string b = "bbb" + std::to_string(a);
        cmd = "insert into T1(A,B) values (" + std::to_string(a) + ", '" + b + "')";
        plnr.ExecuteUpdate(cmd, tx.get());
    }

    cmd = "create table T2(C int, D varchar(9))";
    plnr.ExecuteUpdate(cmd, tx.get());
    std::cout << "Inserting " << n << " records int T2." << std::endl;

    for (int i = 0; i < n; i++) {
        int c = 2 * i;
        std::string d = "ddd" + std::to_string(c);
        cmd = "insert into T2(C,D) values(" + std::to_string(c) + ", '" + d + "')";
        plnr.ExecuteUpdate(cmd, tx.get());
    }

    std::string qry = "select B,D from T1,T2 where A=C";
    auto p = plnr.CreateQueryPlan(qry, tx.get());
    auto s = p->Open();
    while (s->Next()) {
        std::cout << s->GetString("b") << " " << s->GetString("d") << std::endl;
    }
    s->Close();
    tx->Commit();

    cmd2 = "rm -rf " + test_dir;
    system(cmd2.c_str());
}


} // namespace SimpleDB