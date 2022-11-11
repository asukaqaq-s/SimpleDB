#include "log/log_iterator.h"
#include "log/log_manager.h"
#include "recovery/log_record.h"
#include "buffer/buffer_manager.h"
#include "gtest/gtest.h"
#include "recovery/recovery_manager.h"
#include "record/table_heap.h"
#include "record/table_iterator.h"

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <cstring>
#include <algorithm>



namespace SimpleDB {

TEST(TableTest, TablePageTest) {

    // we test insert, update, delete, read in this function
    // because this is just one pages so don't need to travel the entire file
    
    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    std::cout << local_path << std::endl;
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


    Schema sch1;
    std::string test_str = "12345678910";
    Tuple tuple;
    std::vector<Value> vec;
    {
        sch1.AddColumn(Column("a", TypeID::CHAR, 100));
        sch1.AddColumn(Column("b", TypeID::INTEGER));
        sch1.AddColumn(Column("c", TypeID::DECIMAL));
        sch1.AddColumn(Column("d", TypeID::VARCHAR, 120));
        sch1.AddColumn(Column("e", TypeID::INTEGER));

        
        vec.push_back(Value(test_str, TypeID::CHAR));
        vec.push_back(Value(10));
        vec.push_back(Value(121.0));
        vec.push_back(Value(test_str, TypeID::VARCHAR));
        vec.push_back(Value(100));
    
        tuple = Tuple(vec, sch1);
    }


    auto *buffer = buf_manager->NewPage(test_file);
    auto table_page = static_cast<TablePage*>(buffer);
    table_page->InitPage();

    // test insert
    RID rid;
    for (int i = 0;i < 10;i ++) {
        table_page->Insert(&rid, tuple);
        EXPECT_EQ(rid.GetSlot(), i);
    }
    // std::cout << "--------------------------------------------" << std::endl;
    // std::cout << "|  After Insert  |" << std::endl;
    // std::cout << "--------------------------------------------" << std::endl;
    // std::cout << table_page->ToString(sch1) << std::endl;

    // test read
    for (int i = 0;i < 10;i ++) {
        Tuple test_tuple;
        table_page->GetTuple(RID(0, i), &test_tuple);
        EXPECT_EQ(test_tuple, tuple);
    }

    // test delete
    for (int i = 0;i < 10;i ++) {
        RID rid(0,i);
        Tuple tmp_tuple;
        if (i % 2) {
            table_page->Delete(rid, &tmp_tuple);
            EXPECT_EQ(tmp_tuple, tuple);
            EXPECT_EQ(false, table_page->GetTuple(rid, &tmp_tuple));
        }
    }

    // std::cout << "--------------------------------------------" << std::endl;
    // std::cout << "|  After delete  |" << std::endl;
    // std::cout << "--------------------------------------------" << std::endl;
    // std::cout << table_page->ToString(sch1) << std::endl;

    // test update
    for (int i = 0;i < 10;i ++) {
        RID rid(0,i);
        Tuple tmp_tuple;

        if (i % 2 == 0) {
            // generate a new tuple
            Tuple new_tuple;
            {
                std::vector<Value> vec;
                std::string test_str = "999999";
                vec.push_back(Value(test_str, TypeID::CHAR));
                vec.push_back(Value(110));
                vec.push_back(Value(31.0));
                vec.push_back(Value(test_str, TypeID::VARCHAR));
                vec.push_back(Value(90));
                new_tuple = Tuple(vec, sch1);
            }
            

            table_page->Update(rid, &tmp_tuple, new_tuple);
            EXPECT_EQ(tmp_tuple, tuple);
            table_page->GetTuple(rid, &tmp_tuple);
            EXPECT_EQ(tmp_tuple, new_tuple);
        }
    }

    // print 
    // std::cout << table_page->ToString(sch1) << std::endl;
    

    
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}



TEST(TableTest, TableHeapTest) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string test_dir = local_path + "/" + "test_dir";
    std::string test_file = "test1.txt";
    std::string cmd;
    std::cout << local_path << std::endl;
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
    // make schema
    // --------------------
    Schema sch1;
    std::string test_str = "12345678910";
    Tuple tuple;
    std::vector<Value> vec;
    {
        sch1.AddColumn(Column("a", TypeID::CHAR, 100));
        sch1.AddColumn(Column("b", TypeID::INTEGER));
        sch1.AddColumn(Column("c", TypeID::DECIMAL));
        sch1.AddColumn(Column("d", TypeID::VARCHAR, 120));
        sch1.AddColumn(Column("e", TypeID::INTEGER));

        
        vec.push_back(Value(test_str, TypeID::CHAR));
        vec.push_back(Value(10));
        vec.push_back(Value(121.0));
        vec.push_back(Value(test_str, TypeID::VARCHAR));
        vec.push_back(Value(100));
    
        tuple = Tuple(vec, sch1);
    }


    // --------------------
    // init
    // --------------------
    LockManager lock;
    Transaction txn(file_manager.get(), buf_manager.get(), &lock,  1);
    RID rid;
    auto table_heap = std::make_unique<TableHeap>(&txn, test_file, 
                      file_manager.get(), rm.get(), buf_manager.get());
    int insert_tuple_num = 100;
    for (int i = 0;i < insert_tuple_num;i ++) {
        table_heap->Insert(&txn, tuple, &rid);
    }


    // --------------------
    // test scan
    // --------------------
    auto table_iter = table_heap->Begin(&txn);
    int cnt = 0;
    while (!table_iter.IsEnd()) {
        Tuple tmp_tuple = table_iter.Get();
        cnt++;
        ASSERT_EQ(tmp_tuple.GetRID(), table_iter.GetRID());
        table_iter++;
        ASSERT_EQ(tuple, tmp_tuple);
    }
    EXPECT_EQ(cnt, insert_tuple_num);
    

    // ----------------------
    // test delete or update 
    // ----------------------
    std::set<RID> s;
    table_iter = table_heap->Begin(&txn);
    cnt = 0;
    while (!table_iter.IsEnd()) {
        Tuple tmp_tuple = table_iter.Get();
        if (cnt % 2) {
            table_heap->Delete(&txn, tmp_tuple.GetRID());
        }
        else {
            s.insert(tmp_tuple.GetRID());
        }
        cnt++;
        table_iter++;
    }


    // after delete, some tuple should not exist
    cnt = 0;
    table_iter = table_heap->Begin(&txn);
    while (!table_iter.IsEnd()) {
        cnt++;
        if (s.find(table_iter.GetRID()) == s.end()) {
            assert(false);
        }
        table_iter++;
    }
    EXPECT_EQ(cnt, 50);    
// exit(0);
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}


TEST(TableHeapTest, BasicTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 3;
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


    auto colA = Column("colA", TypeID::INTEGER);
    auto colB = Column("colB", TypeID::VARCHAR, 20);
    auto colC = Column("colC", TypeID::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);

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

    
    // --------------------
    //  create table_heap
    // --------------------
    LockManager lock;
    Transaction txn(file_manager.get(), buf_manager.get(), &lock,  1);
    RID rid;
    auto table_heap = std::make_unique<TableHeap>(&txn, test_file, 
                      file_manager.get(), rm.get(), buf_manager.get());
    int tuple_num = 1000;
    
    std::vector<RID> tuple_list(tuple_num);
    
    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        table_heap->Insert(&txn, tuple, &tuple_list[i]);
    }
    
    // scan them
    for (int i = 0; i < tuple_num; i++) {
        auto tmp = Tuple();
        EXPECT_EQ(table_heap->GetTuple(&txn, tuple_list[i], &tmp), true);
        EXPECT_EQ(tmp == tuple, true);
    }


    // update many tuple
    for (int i = 0; i < tuple_num; i++) {
        Tuple tuple;
        if (table_heap->Update(&txn, tuple_list[i], tuple_update) == false) {
            // if inplace updation failed, then we perform the deletion/insertion
            table_heap->Delete(&txn, tuple_list[i]);
            table_heap->Insert(&txn, tuple_update, &tuple_list[i]);
        }
    }
    
    // scan them
    for (int i = 0; i < tuple_num; i++) {
        auto tmp = Tuple();
        EXPECT_EQ(table_heap->GetTuple(&txn, tuple_list[i], &tmp), true);
        EXPECT_EQ(tmp == tuple_update, true);
    }
    
    // delete many tuple
    for (int i = 0; i < tuple_num; i++) {
        table_heap->Delete(&txn, tuple_list[i]);
    }
    
    // scan should fail
    for (int i = 0; i < tuple_num; i++) {
        auto tmp = Tuple();
        EXPECT_EQ(table_heap->GetTuple(&txn, tuple_list[i], &tmp), false);
    }
    
    cmd = "rm -rf " + test_dir;
    system(cmd.c_str());
}

TEST(TableHeapTest, IteratorTest) {
    const std::string filename = "test.db";
    const size_t buffer_pool_size = 3;
    remove(filename.c_str());
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


    auto colA = Column("colA", TypeID::INTEGER);
    auto colB = Column("colB", TypeID::VARCHAR, 20);
    auto colC = Column("colC", TypeID::DECIMAL);
    std::vector<Column> cols;
    cols.push_back(colA);
    cols.push_back(colB);
    cols.push_back(colC);
    auto schema = Schema(cols);

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

    // --------------------
    //  create table_heap
    // --------------------
    LockManager lock;
    Transaction txn(file_manager.get(), buf_manager.get(), &lock,  1);
    RID rid;
    auto table_heap = std::make_unique<TableHeap>(&txn, test_file, 
                      file_manager.get(), rm.get(), buf_manager.get());
    int tuple_num = 1000;
    std::vector<RID> tuple_list(tuple_num);
    

    // insert many tuple
    for (int i = 0; i < tuple_num; i++) {
        table_heap->Insert(&txn, tuple, &tuple_list[i]);
    }


    // scan them
    // since we are not doing concurrent test, we won't get any invalid tuple
    int cnt;
    TableIterator it;
    for (it = table_heap->Begin(&txn), cnt = 0; it != table_heap->End(); ++it, ++cnt) {
        EXPECT_EQ(*it == tuple, true);
        EXPECT_EQ(it->GetRID() == tuple_list[cnt], true);
    }
    EXPECT_EQ(cnt, tuple_num);

    // update many tuple
    for (int i = 0; i < tuple_num; i++) {
        if (table_heap->Update(&txn, tuple_list[i], tuple_update)) {
            // if inplace updation failed, then we perform the deletion/insertion
            table_heap->Delete(&txn, tuple_list[i]);
            table_heap->Insert(&txn, tuple_update, &tuple_list[i]);
        }
    }
    // scan them
    for (it = table_heap->Begin(&txn), cnt = 0; it != table_heap->End(); ++it, ++cnt) {
        EXPECT_EQ(*it == tuple_update, true);
        // don't test the rid, since they may out of order after insertion/deletion
        // EXPECT_EQ(it->GetRID(), tuple_list[cnt]);
    }
    EXPECT_EQ(cnt, tuple_num);

    // delete many tuple
    for (int i = 0; i < tuple_num; i++) {
        table_heap->Delete(&txn, tuple_list[i]);
    }

    // scan should fail
    it = table_heap->Begin(&txn);
    EXPECT_EQ(it, table_heap->End());

}

} // namespace SimpleDB