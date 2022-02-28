# iceberg-lookup-srv

Service library for executing point queries on a sorted Iceberg dataset. Intended to provide a fast and efficient way to
store and query large datasets by in-memory indexing Iceberg Parquet column stats and scanning Parquet files on read.

# Usage

Running example, with:

- Iceberg warehouse Hadoop path: `hdfs://localhost:9000/warehouse`
- Iceberg table: `many_row_groups`
- Column to index: `key`

```bash
mvn compile exec:java -Dexec.mainClass="io.pereira.iceberglookupsrv.Service" -Dexec.args="hdfs://localhost:9000/warehouse many_row_groups key"
```

# Setup

Instructions for creating a dataset. Required local dependencies:

- Scala 2.12
- Spark 3.1
- Hadoop (?)
- Iceberg (?)

## Brew install

```bash
brew install scala@2.12 apache-spark
```

- FIXME: Iceberg gets installed somewhere, but it was not clear when/where.

## HDFS setup

- FIXME: I followed some guides for setting up HDFS on MacOS... I need to link to that!

On MacOS, you need to run it manually with

```bash
/usr/local/Cellar/hadoop/3.3.1/sbin/start-dfs.sh
```

## Writing test datasets to HDFS

Start the Spark shell.

- Assumes Iceberg is available (how?)
- Assumes HDFS is running on `localhost:9000`

```bash
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1\
    --conf spark.driver.extraJavaOptions="-Dderby.system.home=/tmp/derby" \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=hdfs://localhost:9000/warehouse
```

### Dataset with many partitions

Create the dataset

```scala
// For testing purposes, we are using 12-byte column values.
val IdDigestBytes = 12

// Generate a dataset for the target of 40k partitions with 10 records each.
val Partitions = 40000
val RecordsPerPartition = 10
// Create random rows.
var df = sc.range(0, Partitions * RecordsPerPartition).toDF
// Hash the rows for uniformly distributed values with the 12-byte size. 
df = df.select(
  substring(unhex(sha2($"value".cast("STRING"), 0)), 0, IdDigestBytes).as("key"),
  $"value"
)
// Partition and sort the data.
df = df.repartitionByRange(Partitions, $"key")
df = df.sortWithinPartitions($"key")

// Write to the configured `local` catalog into the `testing` table. This saves in `hdfs://localhost:900/warehouse/testing`. 
// df.writeTo("local.testing").create()

// Alternatively, for overwriting.
df.writeTo("local.testing").replace()
```

Checking that it exists (note: this was using a 1 partition example, for brievity)

```
$ hadoop fs -ls -R hdfs://localhost:9000/

drwxr-xr-x   - joey supergroup          0 2022-02-22 07:50 hdfs://localhost:9000/warehouse
drwxr-xr-x   - joey supergroup          0 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing
drwxr-xr-x   - joey supergroup          0 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/data
-rw-r--r--   3 joey supergroup        416 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/data/00000-0-383325ca-cc5f-4e4b-b5fe-b98d6f7cac5f-00001.parquet
drwxr-xr-x   - joey supergroup          0 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/metadata
-rw-r--r--   3 joey supergroup       5714 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/metadata/a58cb3cd-b4ed-4731-9850-a5139efb48ed-m0.avro
-rw-r--r--   3 joey supergroup       3760 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/metadata/snap-2896199280874503898-1-a58cb3cd-b4ed-4731-9850-a5139efb48ed.avro
-rw-r--r--   3 joey supergroup       1768 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/metadata/v1.metadata.json
-rw-r--r--   3 joey supergroup          1 2022-02-22 07:50 hdfs://localhost:9000/warehouse/testing/metadata/version-hint.text
```

### Dataset with many row groups

```scala
// For testing purposes, we are using 12-byte column values.
val IdDigestBytes = 12

// Generate a dataset for the target of many records.
val Records = 100000000
// Create random rows.
var df = sc.range(0, Records).toDF
// Hash the rows for uniformly distributed values with the 12-byte size.
df = df.select(
  substring(unhex(sha2($"value".cast("STRING"), 0)), 0, IdDigestBytes).as("key"),
  $"value"
)
// Partition and sort the data.
df = df.repartition(1)
df = df.sortWithinPartitions($"key")

// Write to the configured `local` catalog into the `testing` table. This saves in `hdfs://localhost:900/warehouse/testing`.
df.writeTo("local.many_row_groups").create()

// Alternatively, for overwriting.
// df.writeTo("local.many_row_groups").replace()
```