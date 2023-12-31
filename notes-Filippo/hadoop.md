# HADOOP - MAPREDUCE
- [HADOOP - MAPREDUCE](#hadoop---mapreduce)
	- [Main classes](#main-classes)
		- [Driver](#driver)
			- [Data types](#data-types)
				- [InputFormat](#inputformat)
				- [OutputFormat](#outputformat)
			- [Multiple inputs](#multiple-inputs)
	- [Multiple inputs](#multiple-inputs-1)
	- [Distributed cache](#distributed-cache)
	- [Patterns](#patterns)
		- [Inverted index](#inverted-index)
		- [Counting with Counters](#counting-with-counters)
		- [Filtering patterns](#filtering-patterns)
			- [Filtering](#filtering)
			- [Top K](#top-k)
			- [Distinct](#distinct)
		- [Data Organization patterns](#data-organization-patterns)
			- [Binning](#binning)
			- [Shuffling](#shuffling)
		- [Metapatterns](#metapatterns)
			- [Job chaining](#job-chaining)
		- [Join patterns](#join-patterns)
	- [SQL operators](#sql-operators)
		- [Selection](#selection)
		- [Projection](#projection)
		- [Union](#union)
		- [Intersection](#intersection)
		- [Others](#others)

## Main classes
### Driver
#### Data types
- org.apache.hadoop.io.Text: like Java String
- org.apache.hadoop.io.IntWritable: like Java Integer
- org.apache.hadoop.io.LongWritable: like Java Long
- org.apache.hadoop.io.FloatWritable : like Java Float

Basic hadoop data types implement the `org.apache.hadoop.io.Writable` and `org.apache.hadoop.io.WritableComparable` interfaces.  
> **keys** must be *comparable* --> implement the WritableComparable interface.  
> **values** are instance of the Writable interface.

New data types can be defined by implementing one of these interfaces.

These methods also are necessary to read correctly the values.
```java
	@Override
	public void readFields(DataInput in) throws IOException {
		income = in.readInt();
		date = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(income);
		out.writeUTF(date);
	}
```
##### InputFormat
- `TextInputFormat`: input for plain text files where the file is broken into lines (\n or \r is used to break the line)
  - *key* will be the position (offset) of the line
  - *value* is the content of the text line
- `KeyValueInputFormat`: input for plain text files which have a separator between the key and a value (such as \t ...)
  - *key* is the text preceding the separator
  - *value* is the text following the separator

##### OutputFormat
- `TextOutputFormat` which emits a line in the format *key*`\t`*value*`\n`

Example in Java:
```java
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MonthIncome.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
```

#### Multiple inputs
In case it is necessary to import different files with different formats, in the arguments can be passed the path of the two files and in the `Driver` class two different mappers must be linked:
```java
  MultipleInputs.addInputPath(job, 
    		inputPath2, // file path
    		TextInputFormat.class, 
    		MapperType2BigData.class); // the mapper for this file
```
> the **Reducer** can (must) be only of one type!

## Multiple inputs
The output of the mappers must be consistent so that they can be analyzed properly.
```java
MultipleInputs.addInputPath(job, new Path(args[1]), 
TextInputFormat.class, Mapper1.class);
MultipleInputs.addInputPath(job, new Path(args[2]), 
TextInputFormat.class, Mapper2.class);
```
Same concept is applicable for `MultipleOutputs` class.

## Distributed cache
- During the initialization of the job, Hadoop creates a "local copy" of the shared/cached files in all nodes that are used to execute some tasks (mappers or reducers) of the job;
- The efficiency of the distributed cache depends on the number of multiple mappers (or reducers) running on the same node/server;
- Without the distributed cache, each mapper/reducer should read, in the setup method, the shared HDFS file; hence, more time is needed because reading data from HDFS is more inefficient;

## Patterns
### Inverted index
Build index from the input data to support faster searches or data enrichment
- Mappers: emit KEY-VALUE pairs where the **key** is set of fields to index and the **value** is the object to associate with each keyword.
- Reducers: concatenate the received values.

Example (practical: word-list of URLs):
```
author1 - book1
author2 - book2     >>      author1 - [book1, book3]
author1 - book3
```

### Counting with Counters
Compute count summarizations of data sets.

> It is a MAP-ONLY job

Examples:
- count number of records
- count small number of unique instances
- summarization

### Filtering patterns
#### Filtering
Filter out input records that are not of interest

> It is a MAP-ONLY job

Example:
- record filtering
- tracking events
- distributed grep
- data cleaning

#### Top K
Select a small set of top K records according to a ranking function

Each mapper initializes its local in-mapper top k list which can be stored in main memory.  
The cleanup method emits the `k` KEY-VALUE pairs associated with the in-mapper local top k pairs.
>> KEY is null (-)

Then, a single reducer must be instantiated since all pairs have the same nullable key and it merges all the top k lists coming from the mappers defining the one which is finally returned.

Example:
```java

```

#### Distinct
Find a unique set of values.

Each mapper emits KEY-VALUE pair.

Each reducer emits just one KEY-VALUE pair.

### Data Organization patterns
Are used to reorganize/split in subsets the input data
#### Binning
To organize/move the input records into categories.

> The input data set contains heterogonous data, but each data analysis usually is focused only on a specific subsets of your data.

>> It is a MAP-ONLY job

#### Shuffling
For anonymization or selecting a random subset of data.

Each mapper emits one KEY-VALUE pair for each input record, where the *KEY is a random number*.

Each reducer emits one KEY-VALUE pair for each value in \[list-of-values] of the input (key, \[list-of-values]) pair. *Is like pick one of them randomly*.

### Metapatterns
#### Job chaining
Execute a sequence of jobs (synchronizing them):
- the output of the job 1 is used by the job 2 as input
- ...

There is a single `Driver.class` which is in charge to setup the workflow and execute the jobs in the proper order.  
Each phase of the chain is a MapReduce job.

Example:
```java

```

### Join patterns
There are possibilities for all joins. Focus is on the natural join.

The goal is to join the content of two relational tables.

There are **2 Mappers**, one for each table that emit KEY-VALUE pair for each input:
- `key`: is the value of the common attribute(s)
- `value`: is the concatenation of the name of the table of the current record and the content of the current record.

The reducer phase instead is done by use of `Iterators` which can iterate over the values just one time. So if you need more iterations over the same value, copy the values into a `List` object.

Example:
```
From:
- (userid=u1, "Users:name=Paolo,surname=Garza")
- (userid=u1, "Likes:movieGenre=horror")

To:
- (userid=u1,"name=Paolo,surname=Garza,genre=horror")
- (userid=u1,"name=Paolo,surname=Garza, genre=adventure")
```

Since typically one of the two tables is small, the **Distributed Cache** approach can be used to provide a *copy of the small table to all mappers*.

## SQL operators
The MapReduce implementations of these methods is efficient only when a full scan of the input table is needed. When queries are *not selective* and process all data.

>> NOTE:
>> In relational algebra duplicates are NOT present by definition. This constraint must be satisfied both for the input and the output tables.
>> In any case, duplicated records are removed during the job.

### Selection
It is a MAP-ONLY job, implemented using the Filtering pattern.

### Projection
The mapper analyzes each record and emits a new one with the column wanted and emits a KEY-VALUE pair where:
- `key` is the new record;
- `value` is null.

The reducer, emits KEY-NULL for each key.

### Union
2 tables --> 2 mappers

The 2 tables must have the same schema.

The mapper1 emits KEY-VALUE pair where
- `key` is the record
- `value` is null

The mapper2 does the same for the other table.

The reducer emits KEY-NULL for each key.

### Intersection
2 tables --> 2 mappers

The 2 tables must have the same schema

There is a record t in the output of the difference operator if and only if t appears in R but not in S.

The mapper1 emits KEY-VALUE pair where
- `key` is the record
- `value` is the name of the relation/table

The mapper2 does the same for the other table.

The reducer emits KEY-NULL pair if the list of values contain only the name of the first table. 

### Others
For join, see Join patterns.  
For aggregation, see Summarization pattern.
