#include "metadata/metadata_manager.h"
#include "gtest/gtest.h"

#include <random>

namespace SimpleDB {

TEST(MetaManagerTest, SimpleTest) {
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
    RecoveryManager rm(&lm);
    BufferManager bm(&fm, &rm, 10);
    LockManager lock;

    std::string mytable = "MyTable";

    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));

    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &bm, &lock, 1); 
    Schema sch = Schema({ Column("A", TypeID::CHAR, 10), Column("B", TypeID::VARCHAR, 20),
                          Column("C", TypeID::INTEGER), Column("D", TypeID::DECIMAL)});
    MetadataManager mdm(true, txn.get(), &fm, &rm, &bm);
    mdm.CreateTable(mytable, sch, txn.get());
    
    
    auto table_info = mdm.GetTable(mytable, txn.get());
    
    // --------------------------------
    // |   Part 1: Table Metadata     |
    // --------------------------------
    auto sch2 = table_info->schema_;
    auto columns = sch2.GetColumns();
    EXPECT_EQ(sch, sch2);
    auto table_info_tmp = mdm.GetTable(mytable, txn.get());
    EXPECT_EQ(table_info, table_info_tmp);
    

    // ----------------------------------
    // |   Part 2: Statistics Metadata  |
    // ----------------------------------
    
    for (int i = 0;i < 50;i ++ ) {
        std::uniform_int_distribution<unsigned> k(0, 50);
        int n = k(engine);std::string r = "rec";
        std::vector<Value> field_list 
        {
            Value(r + std::to_string(n), TypeID::CHAR), // "A"
            Value(r + std::to_string(n), TypeID::VARCHAR), // "B"
            Value(n), // "A"
            Value(double(n)) // "C"
        };
        Tuple tmp_tuple(field_list, sch);
        RID rid;

        table_info->table_heap_->Insert(txn.get(), tmp_tuple, &rid);
    }
    
    StatInfo si = mdm.GetStatInfo(mytable, txn.get());

    std::cout << "B(MyTable) = "  << si.GetAccessBlocks() << std::endl;
    std::cout << "R(MyTable) = "  << si.GetOutputTuples() << std::endl;
    std::cout << "V(MyTable, A) = " << si.GetDistinctVals("A") << std::endl;
    std::cout << "V(Mytable, B) = " << si.GetDistinctVals("B") << std::endl;
    std::cout << "V(Mytable, C) = " << si.GetDistinctVals("C") << std::endl;
    std::cout << "V(Mytable, D) = " << si.GetDistinctVals("D") << std::endl;

    
    // ----------------------------
    // |   Part 3: View Metadata  |
    // ----------------------------
    std::string view_def = "Select B from MyTable where A = 1";
    mdm.CreateView("ViewA", view_def, txn.get());
    std::string v = mdm.GetViewDef("ViewA", txn.get());
    EXPECT_EQ(v, view_def);
    
    // -----------------------------
    // |   Part 4: Index Metadata  |
    // -----------------------------
    mdm.CreateIndex("IndexA", mytable, "A", txn.get());
    mdm.CreateIndex("IndexB", mytable, "B", txn.get());
    auto index_map = mdm.GetIndexInfo(mytable, txn.get());

    IndexInfo ii = index_map["A"];
    std::cout << "B(indexA) = " << ii.GetAccessBlocks() << std::endl;
    std::cout << "R(indexA) = " << ii.GetOutputTuples() << std::endl;
    std::cout << "V(indexA,A) = " << ii.GetDistinctVals("A") << std::endl;
    std::cout << "V(indexA,B) = " << ii.GetDistinctVals("B") << std::endl;

    ii = index_map["B"];
    std::cout << "B(indexB) = " << ii.GetAccessBlocks() << std::endl;
    std::cout << "R(indexB) = " << ii.GetOutputTuples() << std::endl;
    std::cout << "V(indexB,A) = " << ii.GetDistinctVals("A") << std::endl;
    std::cout << "V(indexB,B) = " << ii.GetDistinctVals("B") << std::endl;

        
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

} // namespace SimpleDB