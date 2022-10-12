#include "query/product_scan.h"
#include "gtest/gtest.h"
#include "concurrency/transaction.h"
#include "record/table_scan.h"

#include <random>

namespace SimpleDB {

TEST(ProductScanTest, SimpleTest1) {
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

    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &lm, &bm); 

    // update schema1
    Schema sch1;
    sch1.AddIntField("A");
    sch1.AddStringField("B", 9);
    Layout layout1(sch1);
    TableScan ts1(txn.get(), "T1", layout1);
    
    // update schema2 
    Schema sch2;
    sch2.AddIntField("C");
    sch2.AddStringField("D", 9);
    Layout layout2(sch2);
    TableScan ts2(txn.get(), "T2", layout2);


    int n = 200;
    // ---------------------------
    // | insert tuple to table 1 |
    // ---------------------------
    ts1.FirstTuple();
    std::cout << "Inserting " << n << " records into T1" << std::endl;
    for (int i = 0;i < n;i ++) {
        ts1.Insert();
        ts1.SetInt("A", i);
        ts1.SetString("B", "aaa" + std::to_string(i));
    }
    // ts1.FirstTuple();
    // while(ts1.Next()) {
    //     std::cout << ts1.GetInt("A") << "   " << ts1.GetString("B") << std::endl;
    // }
    ts1.Close();
    // ---------------------------
    // | insert tuple to table 2 |
    // ---------------------------
    ts2.FirstTuple();
    std::cout << "Inserting " << n << " records into T2" << std::endl;
    for (int i = 0;i < n;i ++) {
        ts2.Insert();
        ts2.SetInt("C", n - i - 1);
        ts2.SetString("D", "bbb" + std::to_string(n - i - 1));
    }

    // ts2.FirstTuple();
    // while(ts2.Next()) {
    //     std::cout << ts2.GetInt("C") << "   " << ts2.GetString("D") << std::endl;
    // }
    ts2.Close();

    std::shared_ptr<Scan> s1 = 
        std::make_shared<TableScan> (txn.get(), "T1", layout1);

    // while(s1->Next()) {
    //     std::cout << "ST   " << s1->GetInt("A") << "   " << s1->GetString("B") << std::endl;
    // }
    
    std::shared_ptr<Scan> s2 = 
        std::make_shared<TableScan> (txn.get(), "T2", layout2);
    std::unique_ptr<Scan> s3 = 
        std::make_unique<ProductScan> (s1, s2);
    
    while (s3->Next()) {
        std::cout << s3->GetInt("A") << "  "
                  << s3->GetString("B") << " " 
                  << s3->GetInt("C") << " " 
                  << s3->GetString("D") <<std::endl;
    }
    
    s3->Close();
    txn->Commit();
}

} // namespace SimpleDB