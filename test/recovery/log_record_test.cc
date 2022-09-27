
#include "recovery/log_record.h"
#include "gtest/gtest.h"

namespace SimpleDB {

TEST(LogRecordTest, SetIntTest) {

    BlockId block("txt1.txt", 0);
    SetIntRecord origin_record(1, block, 80, 1, 0);
    auto copy_record = origin_record;

    copy_record.SetCLR();
    origin_record.SetPrevLSN(-1);
    
    std::cout << origin_record.ToString() << std::endl;
    std::cout << copy_record.ToString() << std::endl;
    EXPECT_NE(copy_record, origin_record); 
    
    auto byte_array = origin_record.Serializeto();
    auto page = LogRecord::DeserializeFrom(*byte_array);
    EXPECT_EQ(page->GetLsn(),-1);
    EXPECT_EQ(*(static_cast<SetIntRecord*>(page.get())), origin_record);

    std::cout << origin_record.ToString() << std::endl;
    std::cout << page->ToString() << std::endl;
}

} // namespace SimpleDB