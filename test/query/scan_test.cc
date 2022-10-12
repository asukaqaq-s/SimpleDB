#include "query/product_scan.h"
#include "gtest/gtest.h"
#include "concurrency/transaction.h"
#include "record/table_scan.h"
#include "query/predicate.h"
#include "query/select_scan.h"
#include "query/project_scan.h"

#include <random>

namespace SimpleDB {

TEST(ScanTest, SimpleTest1) {
    int min = 0,max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution
    

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::string testFile = "testfile";
    std::string testFile1 = "testfile.table";

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    FileManager fm(test_dir, 4096);
    
    LogManager lm(&fm, log_file_name);
    
    BufferManager bm(&fm, &lm, 10);

    std::string mytable = "MyTable";

    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));

    std::shared_ptr<Transaction> txn = std::make_shared<Transaction>(&fm, &lm, &bm); 

    // ---------------------------
    // | update schema           |
    // ---------------------------
    Schema sch1;
    sch1.AddIntField("A");
    sch1.AddStringField("B", 9);
    Layout layout(sch1);
    auto s1 = 
    std::static_pointer_cast<UpdateScan>(
    std::make_shared<TableScan>(txn.get(), "T", layout));

    // ---------------------------
    // | insert tuple to table 1 |
    // ---------------------------
    s1->FirstTuple();
    int n = 200;
    std::cout << "INserting  " << n << + " random records." << std::endl;
    
    for (int i = 0;i < n;i ++) {
        s1->Insert();
        std::uniform_int_distribution<unsigned> k(0, 50);
        int x = k(engine);
        s1->SetInt("A", x);
        s1->SetString("B", "rec" + std::to_string(x));
    }
    
    s1->Close();
    
    auto s2 = std::static_pointer_cast<Scan>
        (std::make_shared<TableScan>(txn.get(), "T", layout));
    
    // selecting all records where a = 10
    Constant c(10);
    Expression a("A");
    Expression cc(c);
    Term t(a, cc);
    Predicate pred(t);
    std::cout << "the predicate is " << pred.ToString() << std::endl;
    
    auto s3 = std::static_pointer_cast<Scan>
        (std::make_shared<SelectScan>(s2, pred));
    
    std::vector<std::string> fields;
    fields.emplace_back("A");
    fields.emplace_back("B");
    auto s4 = std::static_pointer_cast<Scan>
        (std::make_shared<ProjectScan>(s3, fields));
    
    // s4->FirstTuple(); 
    // return; 
    while (s4->Next()) {
        std::cout << s4->GetInt("A") << " "
                  << s4->GetString("B") << std::endl;
    }
    s4->Close();
    txn->Commit();
}

TEST(ScanTest, SimpleTest2) {
    int min = 0,max = 100;
    std::random_device seed; // get ramdom seed
	std::ranlux48 engine(seed());
    std::uniform_int_distribution<> distrib(min, max); // uniform distribution
    

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string cmd;
    std::string log_file_name = "log.log";
    std::string log_file_path = test_dir + "/" + log_file_name;
    std::string testFile = "testfile";
    std::string testFile1 = "testfile.table";

    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());

    FileManager fm(test_dir, 4096);
    
    LogManager lm(&fm, log_file_name);
    
    BufferManager bm(&fm, &lm, 10);

    std::string mytable = "MyTable";

    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));

    std::shared_ptr<Transaction> txn = std::make_shared<Transaction>(&fm, &lm, &bm); 

    // ---------------------------
    // | update schema           |
    // ---------------------------
    Schema sch1;
    sch1.AddIntField("A");
    sch1.AddStringField("B", 9);
    Layout layout(sch1);
    
    auto us1 = std::static_pointer_cast<UpdateScan>(
      std::make_shared<TableScan>(txn.get(), "T1", layout));

  us1->FirstTuple();
  int n = 200;
  std::cout << "Inserting " << n << " records into T1." << std::endl;

  for (int i = 0; i < n; i++) {
    us1->Insert();
    us1->SetInt("A", i);
    us1->SetString("B", "bbb" + std::to_string(i));
  }
  us1->Close();

  Schema sch2;
  sch2.AddIntField("C");
  sch2.AddStringField("D", 9);
  Layout lt2(sch2);
  auto us2 = std::static_pointer_cast<UpdateScan>(
      std::make_shared<TableScan>(txn.get(), "T2", lt2));
  us2->FirstTuple();
  std::cout << "Inserting " << n << " records into T2." << std::endl;
  for (int i = 0; i < n; i++) {
    us2->Insert();
    us2->SetInt("C", n - i - 1);
    us2->SetString("D", "ddd" + std::to_string(n - i - 1));
  }
  us2->Close();

  auto s1 = std::static_pointer_cast<Scan>(
      std::make_shared<TableScan>(txn.get(), "T1", layout));
  auto s2 = std::static_pointer_cast<Scan>(
      std::make_shared<TableScan>(txn.get(), "T2", lt2));
  auto s3 = std::static_pointer_cast<Scan>(
      std::make_shared<ProductScan>(s1, s2));

  // selecting all records where A=C
  Term t(Expression("A"), Expression("C"));
  Predicate pred(t);
  std::cout << "The predicate is " << pred.ToString() << std::endl;
  auto s4 = std::static_pointer_cast<Scan>(
      std::make_shared<SelectScan>(s3, pred));

  // projecting on [B,D]
  std::vector<std::string> c = {"B", "D"};
  auto s5 = std::static_pointer_cast<Scan>(
      std::make_shared<ProjectScan>(s4, c));
  while (s5->Next()) {
    std::cout << s5->GetString("B") << " " << s5->GetString("D") << std::endl;
  }
  s5->Close();
  txn->Commit();
    

}

} // namespace SimpleDB