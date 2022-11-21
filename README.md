

[中文版笔记](https://zhuanlan.zhihu.com/p/563388103)

----

## SimpleDB

SimpleDB is a tiny-database-manage-system for learning purpose.Note that this is just a toy for learning and is NOT a robust and full solution to Database.

## Feature of SimpleDB

I finish some features which different from the original version of SimpleDB.

Listed below:

**File Manager & Log Manager**: 

- File manager separate the log read/write from the table read/write.By append mode of fstream, reading and writing logs will be more efficient.

**Memory Manager**: 

- Bufferpool supports LRU algorithm to find a victim.
- The page table of bufferpool will help us find a buffer in the bufferpool only requires O1 time complexity.
- We only write a page to disk when it becomes a victim even if the pin_count of page is reduced to 0.

**Transaction Manager**:

- Support Wound-Wait deadlock prevent algorithm to resolve deadlock problems.
- But since I want to use row-level-locking, still doesn't solve the phantom reading problem, only support read-repeatable isolation level.

**Recovery Manager**:

- Crash recovery process will be more efficient by ARIES algorithm and Fuzzy Checkpoint 

**Record Manager**:

- Tuple oritened structure will support varlen record and reduce space waste.

**Metadata Manager**:

- There are no additional features

**Query Processing**:

- support >=, <, >, <=, != 
- support some simple arithmetic op.
- support and, or.

**paring**:

- after parsing, we should verify correctness of SQL syntactic. 

**Planning**:

- ...

**Index**:

- SimpleDB support 3 Index data structures: static hash table, extendible hash table and b plus tree hash table.
- I have tried crab-concurrency-scheme in btree, but it doesn't seem to be efficient, and there are no new changes at the moment.


## TODOPlan

- Parser, planner and Verifyer
- Row level locking to replace Page level locking and implement serializable isolation level by row level locking.
- Aggregation Function
- Index update log record. 



## reference

**lecture**: 

- [cmu 15-445](https://15445.courses.cs.cmu.edu/fall2022/)

**books**:

- [database design and implementation](www.cs.bc.edu/~sciore/simpledb/)

**github**:

- [SimpleDB implemented by C++ （rewrite)](​github.com/wattlebirdaz/simpledb)
- [ysj1173886760/TinyDB: TinyDB](​github.com/ysj1173886760/TinyDB)

