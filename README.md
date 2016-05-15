# Spark Sorting & Caching #

Practice in utilizing a few features in Spark:

* `repartitionAndSortWithinPartition`
* `broadcast` for caching values that are frequently used
* custom output writer based on `MultipleTextOutputFormat`

## Running The Program ##

### Requirements ###
1. SBT 0.13.8
2. Java(TM) SE Runtime Environment 1.8.0_74
3. Scala 2.10.6
4. Spark Core 1.6.1

### Input Files ###

1. There are a few log files under files/logs/, log1.txt to log40.txt. Each file contains ±1000 lines. The numbers are randomly generated and non-unique.
2. There are a few country in a mapping file located at files/countryCodes.txt.
3. After a run, the output will be under files/out/. This directory must be deleted before a subsequent run.

### Steps ###
```
#!bash

git clone https://github.com/lolski/practice-bigdata-spark.git
cd practice-bigdata-spark
rm -fr ./files/out # DELETE 'out' directory before executing a run
sbt run
```