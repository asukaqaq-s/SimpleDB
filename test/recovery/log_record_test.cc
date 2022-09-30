
#include "recovery/log_record.h"
#include "gtest/gtest.h"

namespace SimpleDB {

TEST(LogRecordTest, SetIntTest) {

    BlockId block("txt1.txt", 0);
    SetIntRecord origin_record(3, block, 80, 122, 0222);
    auto copy_record = origin_record;

    copy_record.SetCLR();
    origin_record.SetPrevLSN(100);
    origin_record.SetLsn(200);
    // std::cout << origin_record.ToString() << std::endl;
    // std::cout << copy_record.ToString() << std::endl;
    EXPECT_NE(copy_record, origin_record); 
    
    auto byte_array = origin_record.Serializeto();
    auto page = LogRecord::DeserializeFrom(*byte_array);
    EXPECT_EQ(page->GetLsn(),200);
    EXPECT_EQ(*(static_cast<SetIntRecord*>(page.get())), origin_record);
    
    // std::cout << origin_record.ToString() << std::endl;
    // std::cout << page->ToString() << std::endl;

    std::shared_ptr<Page> p = std::make_shared<Page>(byte_array);
    SetIntRecord test_record(p.get());
    
    EXPECT_EQ(test_record, origin_record);

}

TEST(LogRecordTest, SetStringTest) {

    BlockId block("txt1.txt", 0);
    SetStringRecord origin_record(3, block, 80, "12223", "445562");

    auto copy_record = origin_record;

    copy_record.SetCLR();
    origin_record.SetPrevLSN(100);
    origin_record.SetLsn(200);
    std::cout << origin_record.ToString() << std::endl;
    std::cout << copy_record.ToString() << std::endl;
    EXPECT_NE(copy_record, origin_record); 
    
    auto byte_array = origin_record.Serializeto();
    auto page = LogRecord::DeserializeFrom(*byte_array);
    EXPECT_EQ(page->GetLsn(),200);
    EXPECT_EQ(*(static_cast<SetStringRecord*>(page.get())), origin_record);
    
    std::cout << origin_record.ToString() << std::endl;
    std::cout << page->ToString() << std::endl;

    std::shared_ptr<Page> p = std::make_shared<Page>(byte_array);
    SetStringRecord test_record(p.get());
    
    EXPECT_EQ(test_record, origin_record);

}

} // namespace SimpleDB