#include "record/table_page.h"
#include "gtest/gtest.h"
#include "buffer/buffer_manager.h"
#include "file/file_manager.h"
#include "log/log_manager.h"
#include "record/table_scan.h"

#include <random>
#include <memory>

namespace SimpleDB {

TEST(TableScanTest, SimpleTest1) {

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

    // bm.NewPage(BlockId(testFile1, 0));
    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));

    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &lm, &bm); 

    Schema sch = Schema();
    sch.AddIntField("A");
    sch.AddStringField("B", 9);
    Layout layout = Layout(sch);
    for (std::string fldname : layout.GetSchema().GetFields()) {
        int offset = layout.GetOffset(fldname);
        std::cout << fldname + " has offset " + std::to_string(offset) << std::endl;
    }
    std::cout << "Filling the table with 50 random records." << std::endl;
    
    TableScan ts = TableScan(txn.get(), "testfile", layout);
    // return ;
    for (int i=0; i< 50;  i++) {
        std::uniform_int_distribution<unsigned> k(0, 50);
        ts.Insert();
        int n = k(engine);
        ts.SetInt("A", n);
        ts.SetString("B", "rec" + std::to_string(n));
        std::cout << "inserting into slot " + ts.GetRid().ToString() + ": {" + 
            std::to_string(n) + ", " + "rec"+ std::to_string(n) + "}" << std::endl;
        
    }
    std::cout << "Deleting these records, whose A-values are less than 25." << std::endl;
      int count = 0;
      ts.FirstTuple();

    while (ts.Next()) {
        int a = ts.GetInt("A");
        std::string b = ts.GetString("B");
        if (a < 25) {
            count++;
            std::cout << "slot " + ts.GetRid().ToString() + ": {" + std::to_string(a) + ", " + 
            b + "}" << std::endl;
            ts.Remove();    
        }
    }

    std::cout << count << " values under 10 were deleted.\n";

    std::cout << "Here are the remaining records." << std::endl;

      ts.FirstTuple();
      while (ts.Next()) {
         int a = ts.GetInt("A");
         std::string b = ts.GetString("B");
            std::cout << "slot " + ts.GetRid().ToString() + ": {" + std::to_string(a) + ", " + b + "}" << std::endl;
      }

    ts.Close();
    txn->Commit();
}

}