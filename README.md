### Creating external table in hive for weather files in HDFS
`CREATE EXTERNAL TABLE weather_external (
lng DOUBLE,  
lat DOUBLE,  
avg_tmpr_f DOUBLE,
avg_tmpr_c DOUBLE,
wthr_date STRING)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION "hdfs://localhost:9000/201 HW Dataset/weather";
`
### To make partitions work

`MSCK REPAIR TABLE weather_external;`

### Creating a topic with 8 partitions for weather in kafka;

`kafka-topics.sh --create --zookeeper my-release-kafka-zookeeper:2181 --replication-factor 1 --partitions 8 --topic weather`

### Creating weather external for kafka handling
`CREATE EXTERNAL TABLE weather_kafka (
lng DOUBLE,
lat DOUBLE,
avg_tmpr_f DOUBLE,
avg_tmpr_c DOUBLE,
wthr_date STRING,
year STRING,
month STRING,
day STRING)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES(
"kafka.topic" = "weather",
"kafka.bootstrap.servers"="localhost:9094"
);`

### Inserting from weather_external into weather_kafka with 8 partitions;
``FROM weather_external
INSERT INTO TABLE weather_kafka
SELECT
lng AS `lng`,
lat AS `lat`,
avg_tmpr_f AS  `avg_tmpr_f`,
avg_tmpr_c AS `avg_tmpr_c`,  
wthr_date AS `wthr_date`,
year AS `year`,  
month AS `month`,
day AS `day`,
null AS `__key`,
cast(day as int)%8 AS `__partition`,
-1 AS  `__offset`,
-1 AS `__timestamp`;``

### I decided to find unique dates among the weather with hive, so:
### Creating external table for unique days:
`CREATE EXTERNAL TABLE days_kafka (
wthr_date STRING,
year STRING,
month STRING,
day STRING)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES(
"kafka.topic" = "daysUnique",
"kafka.bootstrap.servers"="localhost:9094"
);`
### And insert into there…:
``INSERT INTO TABLE days_kafka
SELECT distinct wthr_date,  
year,
month,
day,
'dummy' AS `__key`,
null as `__partition`,
-1 AS  `__offset`,
-1 AS `__timestamp`
FROM weather_external;``

`Select count(*) from days_kafka` will show us 92 unique dates:

`+------+`  
`| _c0  |`  
`+------+`  
`| 92   |`  
`+------+`


### Creating topic for joined hotelWeather data:

#### NOTE: I made a decision of log compaction for reducing the number of old data per key.
In the app I use KTable as a final result with commit interval of 400 sec.

`kafka-topics.sh --create --zookeeper my-release-kafka-zookeeper:2181 --replication-factor 1 --partitions 1 --topic hotelDailyData --config "cleanup.policy=compact" --config "delete.retention.ms=70000"  --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01"  
`  

### Creating external table for finalData (optional) I used it for debugging

`CREATE EXTERNAL TABLE hotelDailyData (
Id BIGINT,
name STRING,
country STRING,
city STRING,
Address STRING,
Latitude  DOUBLE,
Longitude DOUBLE,
GeoHash STRING,
wthr_date DATE,
day STRING,
month STRING,
year STRING,
avg_tmpr_f DOUBLE,
avg_tmpr_c DOUBLE)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES(
"kafka.topic" = "hotelDailyData",
"kafka.bootstrap.servers"="localhost:9094"
);`

### Launch the app with 8 threads.
Executing `select count(*) from hotelDailyData ` gives us `233578` results;

### Check for duplicates query:
`select * from (select count (id) as cnt, name, wthr_date from hotelDailyData group by id,name, wthr_date) as wtf where cnt  >1; `  
Result: `4,130 rows selected`  

So, there are 4130 rows duplicates (containing not the last data for a given key)
Neither log compaction nor commit interval of 400 sec gave me the desired result – only 1 record per key.

But if we make some calculations..:

92 unique dates, 2494 hotels ->  `2494x92 +4130 = 233578 `(records) that we see in kafka.
