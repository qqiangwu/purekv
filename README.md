# What's this
An extremely simple LSM storage engine, used for illustration purpose only. The code is written I am reading the code of [LevelDB](https://github.com/google/leveldb)

# Principles
This code is intended for illustration purpose, so efficiency is not taken into consideration.

+ Batch operations are not considered
+ Group commit is not considered
+ All log appends will be synchronized