
# BigDataCraftSales Introduction
Given two data sets in HDFS as mentioned below.
o Customer information &lt;customer_id,name,street,city,state,zip&gt;
o Sales information &lt;timestamp,customer_id,sales_price&gt;

Using Spark Core, BigDataCraftSales provides the total sale based on State and for (year, month, day and hour) granularities

##  Data Analysis

 This dataset is analyzed to provide the following information:

You have given two data sets in HDFS as mentioned below.
o Customer information &lt;customer_id,name,street,city,state,zip&gt;
o Sales information &lt;timestamp,customer_id,sales_price&gt;

# Data Pipeline

![Data pipeline](images/datapipeline.JPG)

## Historical Data Analysis Details

### Data Input and Output Format

Example Input: (Assume input as text format with &#39;#&#39; as delimiter)

customers:
123#AAA Inc#1 First Ave Mountain View CA#94040
456#ABC Inc#2 First Ave Fayetteville AK#72703
789#DEF Inc#3 First Ave Mobile AL#36571
101112#GHI Inc#4 First Ave Portland OR#97205
Sales:
1454313600#123#123456
1501578000#789#123457
1470045600#456#123458
1470049200#789#123459

Example Output:
state#year#month#day#hour#sales
CA#2016#02#01#08#123456
CA#2016#02#01##123456
CA#2016#02###123456
CA#2016####123456
CA#####123456
AL#2017#08#01#09#123457



### Data Ingestion

The Ingestion is done in Spark reads data files from HDFS storage and parses data and joins dataset using Spark Core and stores resultsData in Cassandra. 
Then results for each state are retreived and combined using Spark and stored in output file in HDFS.

### Data Analyzer

The data analysis and results preparation is handled by the BigDataSalesAnalyzer class.

#### BigDataSalesAnalyzer

This class initializes the Java Spark Context and uses SalesAnalyzerFactory to get instances of the Analysis and Results processing classes. It also validates the paramters.

The CustomerRow and SalesRow classes denotes a row of data in the respective input datasets.

#### SalesAnalyzerFactory

This defines the Strategy enum and uses that to create the appropriate instance of Analyzer and Results implementation classes.

#### SalesAnalyzer

This interface defines the basic methods in the Analyzer.

##### SalesAnalyzerSimple

This class implements a basic outline of the analyzer processing that can be overridden in other implementors based on their strategy.

It loads the Customer data as RDD and converts to CustomerRow RDD. Same is done for Sales data to load the SalesRow RDD. These RDDs can be filtered to remove any rows that aren't valid.

After this, the two RDDs are joined simply using the join method and mapped to AnalyzerRow class.

AnalyzerRow represents the results of Analysis and is handy in creating the needed data. The joined RDD is used to create the multiple statistics by using reduceByKey on multiple Date related keys created as Tuples and results are stored in HDFS.

##### SalesAnalyzerCassandra

This class implements the default strategy by extending the SalesAnalyzerSimple class. It re-implements the storeAnalysis method so that the results are persisted to Cassandra Table craftanalysis.

##### SalesAnalyzerCustSmall

This class implements the small Customer dataset strategy (cust_small). It assumes that the Customer data will fit in memory and so the join can optimized as a Broadcast Hash join. 

This is done by converting the customerRDD to Map using collectAsMap and then broadcasting it. Then mapPartitions is called on salesRDD with a FlatMapFunction to join with the customer data in memory and the AnalyzerRow object is created for each join.

This will greatly improve performance in this scenario as it avoid the shuffly or the regular join method.

#### SalesResults

This interface defines the basic method in the Result processor.

##### SalesResultSimpler

This class provides an empty no-op implmentation since it is very cumbersome to produce the required input from HDFS.

##### SalesResultsCassandra

This class uses the Cassandra clustering implicit sorting to produce the results effortlessly. It uses a pre-defined sorted states list to do multiple queries on Cassandra table. Each RDD is already returned in the needed sorted manner. These are union'ed and then re-partitioned into 1 partition to get a single file which is stored in HDFS. 

### Cassandra Schema

```sql
CREATE TABLE craft.craftanalysis (
   state text,
   epoch timestamp,
   year text,
   month text,
   day text,
   hour text,
   amount decimal,
   PRIMARY KEY ((state), epoch)
) WITH CLUSTERING ORDER BY (epoch DESC);
```

# Technologies Details

It is built using the following technologies:

- Spark 2.1.0
- Apache Cassandra 3
- Apache HDFS

# Running BigDataCraftSales
- Copy Customer and Sales input dataset in HDFS
- Create craft.craftanalysis in Cassandra
- Submit spark job from terminal


# Problem : Introduction to BigData Craft Problem

You have given two data sets in HDFS as mentioned below.
o Customer information &lt;customer_id,name,street,city,state,zip&gt;
o Sales information &lt;timestamp,customer_id,sales_price&gt;
Implement a Spark application using Spark Core (not with Spark SQL) to
get &lt;state,total_sales&gt; for (year, month, day and hour) granularities. Be
prepared to present your code and demonstrate it running with appropriate
input and output.
Notes:
1. You can consider input/output data set in any one of the below format
a. Text(with any DELIMITER)
b. AVRO
c. PARQUET
2. Consider timestamp in epoch (for example 1500674713)
3. We encourage you to consider all possible cases of datasets like
number of states are small(finitely known set) OR huge(unknown set)
and come with appropriate solutions.
4. You can you use any of PYTHON/SCALA/JAVA API’s of your
choice.

Example Input: (Assume input as text format with &#39;#&#39; as delimiter)

customers:
123#AAA Inc#1 First Ave Mountain View CA#94040
456#ABC Inc#2 First Ave Fayetteville AK#72703
789#DEF Inc#3 First Ave Mobile AL#36571
101112#GHI Inc#4 First Ave Portland OR#97205
Sales:
1454313600#123#123456
1501578000#789#123457
1470045600#456#123458
1470049200#789#123459

Example Output:
state#year#month#day#hour#sales

AL#2017#08#01#09#123457
AL#2017#08#01##123457
AL#2017#08###123457
AL#2017####123457
AL#2016#08#01#11#123459
AL#2016#08#01##123459
AL#2016#08###123459
AL#2016####123459
AL#####246916
AK#2016#08#01#10#123458
AK#2016#08#01##123458
AK#2016#08###123458
AK#2016####123458
AK#####123458

CA#2016#02#01#08#123456
CA#2016#02#01##123456
CA#2016#02###123456
CA#2016####123456
CA#####123456


