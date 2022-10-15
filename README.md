

[中文版笔记](https://zhuanlan.zhihu.com/p/563388103)

----

## SimpleDB

SimpleDB is a tiny-database-manage-system for learning purpose.Note that this is just a toy for learning and is NOT a robust and full solution to Database.

Schedule 

- FileManager (comepleted in 9/16/2022)
- BufferManager(completed in 9/23/2022)
- Recover by WAL(completed in 1/10/2022)
- Transaction by 2pl(completed in 1/10/2022)
- Record Manager(completed in 5/10/2022)
- Metadata Manager(completed in 7/10/2022)
- Query(completed in 12/10/2022)
- Parsing(completed in 14/10/2022)
- Planning(todo)
- JDBC(todo)
- Indexing(todo)
- Materialization and Sorting(todo)
- Effective buffer Utilization(todo)
- Query Optimization(todo)


## important todo

- asynchronous IO in LogManager

- ARIES: Currently, the recovery mechanism still uses non-fuzzy checkpoint.I will update it to ARIES and FUZZY-checkpoint soon.

- DeadLock: Currently, I haven't dealt with how to resolve a deadlock problem.I will update it to WOUND-WAIT or WAIT-DIE.

- TablePage: 
    
    - we should support variable length tuple.Currently, we just implement slotted page on fixed-length tuple.
    - of course, i think support char、varchar、blob type is necessary.

- StatManager: 

    - Can we make a background transaction or a transaction which ioslation level is "READ UNCOMMITED ?" 
    - Can we maintain more informations which help planner working

- Query:

    - not only ==, we should also implement <,>,!=,- and so on
    - not only 'and', we should also implement 'or', 'not'
    - support computation, sorting, grouping, nextsing, renaming

- Update:
  
    - we should support drop table
    - can we support change database?
  
- Planner:

    - planner should support big table query so that filter more early
    - planner should have more efficient optimize.
  

## reference

**lecture**: 

- [cmu 15-445](https://15445.courses.cs.cmu.edu/fall2022/)

**books**:

- [database design and implementation](www.cs.bc.edu/~sciore/simpledb/)

**github**:

- [SimpleDB implemented by C++ （rewrite)](​github.com/wattlebirdaz/simpledb)
- [ysj1173886760/TinyDB: TinyDB](​github.com/ysj1173886760/TinyDB)

