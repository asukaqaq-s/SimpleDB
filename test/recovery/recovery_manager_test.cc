#include "recovery/log_record.h"
#include "recovery/recovery_manager.h"
#include "gtest/gtest.h"

#include <memory>

namespace SimpleDB {

TEST(RecoveryTest, LogTest) {

    char buf[100];
    std::string local_path = getcwd(buf, 100);
    std::string directory_path = local_path + "/test_directory";
    int block_size = 4 * 1024;
    std::string file_name = "test1.txt";
    FileManager fm(directory_path, block_size);
    LogManager lm(&fm, "log.log");
    BufferManager bm(&fm, &lm, 10);

    std::unique_ptr<RecoveryManager> rm = std::make_unique<RecoveryManager>(nullptr, 1, &lm, &bm);
    
    auto buffer = bm.NewPage(file_name);
    rm->SetIntLogRec(buffer, 10, 10);
    rm->SetStringLogRec(buffer, 20, "qaq");
    auto lsn = rm->SetIntLogRec(buffer, 40, 20);
    lm.Flush(lsn);
    rm->RollBack();
    
    auto log_it = lm.Iterator();
    while(log_it.HasNextRecord()) {
        auto byte_array = log_it.NextRecord();
        auto rec = LogRecord::DeserializeFrom(byte_array);
        
        std::cout << rec->ToString() << std::endl;
    }
    
}


} // namespace SimpleDB