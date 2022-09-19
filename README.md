# Apache Hudi tute

```
conda create -n hudi-tute python=3.10
conda activate hudi-tute

pip install -r requirements.txt

which pyspark

pyspark --help

pyspark \
    --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
    --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
```


## PySpark Shell

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.3.0
      /_/

Using Python version 3.10.6 (main, Aug 22 2022 20:41:54)
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = local[*], app id = local-1663321359058).
SparkSession available as 'spark'.
>>>
```

- Observe http://localhost:4040


## Hudi Table

_... while at PySpark Shell, continue to create "Hudi Table" like so:_

```
>>> data = spark.range(0, 5)

>>> data
DataFrame[id: bigint]

>>> type(data)
<class 'pyspark.sql.dataframe.DataFrame'>

>>> data.printSchema()
root
 |-- id: long (nullable = false)

>>> tblname = "mytbl"

>>> tblpath = "file://" + os.getcwd() + "/out/mytbl"

>>> hudi_options = { 
    'hoodie.table.name': tblname,
    'hoodie.datasource.write.recordkey.field': "id",
    'hoodie.datasource.write.partitionpath.field': "",
    'hoodie.datasource.write.precombine.field': "id",
}

>>> data.write.format("hudi").options(**hudi_options).save(tblpath)

>>> df = spark.read.format("hudi").load(tblpath)

>>> df
DataFrame[_hoodie_commit_time: string, _hoodie_commit_seqno: string, _hoodie_record_key: string, _hoodie_partition_path: string, _hoodie_file_name: string, id: bigint]

>>> df.printSchema()
root
 |-- _hoodie_commit_time: string (nullable = true)
 |-- _hoodie_commit_seqno: string (nullable = true)
 |-- _hoodie_record_key: string (nullable = true)
 |-- _hoodie_partition_path: string (nullable = true)
 |-- _hoodie_file_name: string (nullable = true)
 |-- id: long (nullable = false)

>>> df.show()
+-------------------+--------------------+------------------+----------------------+--------------------+---+
|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id|
+-------------------+--------------------+------------------+----------------------+--------------------+---+
|  20220918084234339|20220918084234339...|                 0|  __HIVE_DEFAULT_PA...|51893a93-9466-4b1...|  0|
|  20220918084234339|20220918084234339...|                 1|  __HIVE_DEFAULT_PA...|51893a93-9466-4b1...|  1|
|  20220918084234339|20220918084234339...|                 2|  __HIVE_DEFAULT_PA...|51893a93-9466-4b1...|  2|
|  20220918084234339|20220918084234339...|                 3|  __HIVE_DEFAULT_PA...|51893a93-9466-4b1...|  3|
|  20220918084234339|20220918084234339...|                 4|  __HIVE_DEFAULT_PA...|51893a93-9466-4b1...|  4|
+-------------------+--------------------+------------------+----------------------+--------------------+---+

>>> data = spark.range(5, 10)

>>> data.write.format("hudi").options(**hudi_options).mode("overwrite").save(tblpath)

>>> df = spark.read.format("hudi").load(tblpath)

>>> df.show()
+-------------------+--------------------+------------------+----------------------+--------------------+---+
|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id|
+-------------------+--------------------+------------------+----------------------+--------------------+---+
|  20220918085848090|20220918085848090...|                 5|  __HIVE_DEFAULT_PA...|b898f6ba-6e16-491...|  5|
|  20220918085848090|20220918085848090...|                 6|  __HIVE_DEFAULT_PA...|b898f6ba-6e16-491...|  6|
|  20220918085848090|20220918085848090...|                 7|  __HIVE_DEFAULT_PA...|b898f6ba-6e16-491...|  7|
|  20220918085848090|20220918085848090...|                 8|  __HIVE_DEFAULT_PA...|b898f6ba-6e16-491...|  8|
|  20220918085848090|20220918085848090...|                 9|  __HIVE_DEFAULT_PA...|b898f6ba-6e16-491...|  9|
+-------------------+--------------------+------------------+----------------------+--------------------+---+

>>> exit()
```

```
$ tree -a out/mytbl

out/mytbl
├── .hoodie
│   ├── .20220918085848090.commit.crc
│   ├── .20220918085848090.commit.requested.crc
│   ├── .20220918085848090.inflight.crc
│   ├── .aux
│   │   └── .bootstrap
│   │       ├── .fileids
│   │       └── .partitions
│   ├── .hoodie.properties.crc
│   ├── .schema
│   ├── .temp
│   ├── 20220918085848090.commit
│   ├── 20220918085848090.commit.requested
│   ├── 20220918085848090.inflight
│   ├── archived
│   ├── hoodie.properties
│   └── metadata
│       ├── .hoodie
│       │   ├── .00000000000000.deltacommit.crc
│       │   ├── .00000000000000.deltacommit.inflight.crc
│       │   ├── .00000000000000.deltacommit.requested.crc
│       │   ├── .20220918085848090.deltacommit.crc
│       │   ├── .20220918085848090.deltacommit.inflight.crc
│       │   ├── .20220918085848090.deltacommit.requested.crc
│       │   ├── .aux
│       │   │   └── .bootstrap
│       │   │       ├── .fileids
│       │   │       └── .partitions
│       │   ├── .heartbeat
│       │   ├── .hoodie.properties.crc
│       │   ├── .schema
│       │   ├── .temp
│       │   ├── 00000000000000.deltacommit
│       │   ├── 00000000000000.deltacommit.inflight
│       │   ├── 00000000000000.deltacommit.requested
│       │   ├── 20220918085848090.deltacommit
│       │   ├── 20220918085848090.deltacommit.inflight
│       │   ├── 20220918085848090.deltacommit.requested
│       │   ├── archived
│       │   └── hoodie.properties
│       └── files
│           ├── ..files-0000_00000000000000.log.1_0-0-0.crc
│           ├── ..files-0000_00000000000000.log.1_0-56-1246.crc
│           ├── ..files-0000_00000000000000.log.2_0-87-2464.crc
│           ├── ..hoodie_partition_metadata.crc
│           ├── .files-0000_00000000000000.log.1_0-0-0
│           ├── .files-0000_00000000000000.log.1_0-56-1246
│           ├── .files-0000_00000000000000.log.2_0-87-2464
│           └── .hoodie_partition_metadata
└── __HIVE_DEFAULT_PARTITION__
    ├── ..hoodie_partition_metadata.crc
    ├── .b898f6ba-6e16-491b-bcad-b4df654120c8-0_0-77-2458_20220918085848090.parquet.crc
    ├── .hoodie_partition_metadata
    └── b898f6ba-6e16-491b-bcad-b4df654120c8-0_0-77-2458_20220918085848090.parquet

20 directories, 34 files
```

Reading:
- https://hudi.apache.org/docs/timeline  (go through "**Hudi Concepts**" chapter)
- https://hudi.apache.org/tech-specs/


## Quickstart Notebook

```
$ jupyter-lab
(CTRL + C)
```

- Go to http://localhost:8888/lab
- Open [quickstart.ipynb](quickstart.ipynb) in JupyterLab
- Execute each Notebook cells (_Shift + Enter_) -- one by one to observe

REF:
- https://hudi.apache.org/docs/quick-start-guide/


## Notes

- Key takeaway notes 
  - Required _mandatory fields_ to create a Hudi table

    ```
    'hoodie.table.name': "tblname",
    'hoodie.datasource.write.recordkey.field': "id",
    'hoodie.datasource.write.partitionpath.field': "path",
    'hoodie.datasource.write.precombine.field': "ts",
    ```

  - Strong design choice made on tracking high [velocity](https://www.google.com/search?q=big+data+4+vs) "timestamped-data"


### Re-Spin

```
rm -rf out/*tbl
```

### Related

- https://github.com/victorskl/deltalake-tute
- https://github.com/victorskl/iceberg-tute
