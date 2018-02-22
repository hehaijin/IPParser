The program reads from a log file of access records, loads record into a database, and do some query on records. 

Notes:

1, In real practice I think it might be better to put loading the whole access log and the blocked IP data into seperate tools. But here, to avoid repeat upload, after each new run on a log file, the program is going to put a "processed" mark in the file, so next time it will not upload whole file to database again. The IP blocking is not affected by this.

2, SQL schema. the log table uses a auto-increment key as id, as I found using IP + time can not gararantee to be unique.  For the blocked ip data, IP+ startTime+ duration should be unique, so I use them as primary key. And in case of run the same parameter on the log file again, it will not lead to more insertions. 

3, The log table has more than 110K rows, so I used batch update. The blocked table is small, so just insert one by one.

4, for simplicity, the database is a AWS relational dabase instance. The entry is "ipparser.c3fxdgnnonos.us-east-2.rds.amazonaws.com/IPParser", user: administrator, password: abcdefgh.

But may be due to network conditions, the insertion speed in batch mode is about 4000/s. So it takes about 30s for the program to run on the sample log file. 

5, external libraries used include: Joda-time, MySQL-connector-java.