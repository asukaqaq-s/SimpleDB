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

    std::string mytable = "MyTable";

    // bm.NewPage(BlockId(testFile1, 1));
    // bm.NewPage(BlockId(testFile1, 2));

    std::unique_ptr<Transaction> txn = std::make_unique<Transaction>(&fm, &bm, &rm); 

    Schema sch = Schema();
    sch.AddIntField("A");
    sch.AddVarStrField("B", 9);
    sch.AddDemField("C");
    sch.AddStrField("D", 10);
    
    MetadataManager mdm(true, txn.get());
    mdm.CreateTable(mytable, sch, txn.get());
    
    
    Layout layout = mdm.GetLayout(mytable, txn.get());
    int size = layout.GetLength();
    
    // --------------------------------
    // |   Part 1: Table Metadata     |
    // --------------------------------
    Schema sch2 = layout.GetSchema();
    
    std::cout << "MyTable has fix-layout size " << size << std::endl;
    std::cout << "Its fields are: " << std::endl;
    
    

    for(auto t:sch2.GetFields()) {
        std::string type_str;
        auto type = sch2.GetType(t);
        
        int strlen;
        switch(type) {
        
        case TypeID::INTEGER:
            type_str = "int";
            break;
        
        case TypeID::DECIMAL:
            type_str = "double";
            break;
        
        case TypeID::VARCHAR:
            strlen = sch2.GetLength(t);
            type_str = "varchar(" + std::to_string(strlen) + ")";
            break;
        case TypeID::CHAR:
            strlen = sch2.GetLength(t);
            type_str = "char(" + std::to_string(strlen) + ")";
            break;

        default:
            break;
        }

        std::cout << type_str << std::endl;
    }

   
    // ----------------------------------
    // |   Part 2: Statistics Metadata  |
    // ----------------------------------
    TableHeap ts(txn.get(), mytable, layout);
    
    for (int i = 0;i < 50;i ++ ) {
        std::uniform_int_distribution<unsigned> k(0, 50);
        int n = k(engine);std::string r = "rec";
        std::vector<Value> field_list {
            Value(n), // "A"
            Value(r + std::to_string(n), TypeID::VARCHAR), // "B"
            Value(double(n)), // "C"
            Value(r + std::to_string(n), TypeID::CHAR), // "D"
        };
        Tuple tmp_tuple(field_list, layout);
        RID rid;

        ts.Insert(tmp_tuple, &rid);

        std::cout << tmp_tuple.ToString(layout) << std::endl;
    }
    
    StatInfo si = mdm.GetStatInfo(mytable, layout, txn.get());

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
    std::cout << "View def = " << v << std::endl;
    
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

    txn->Commit();
    
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

} // namespace SimpleDB