CREATE TABLE 
transaction_test (column_1 string, column_2 string)
CLUSTERED BY (column_1) INTO 1 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC
TBLPROPERTIES('COLUMN_STATS_ACCURATE' = 'true', 'orc.compress' = 'ZLIB', 'transactional' = 'true');
