create database my_schema;

DROP TABLE IF EXISTS my_schema.result;
CREATE TABLE my_schema.result (year STRING, value INT)
  stored as sequencefile
;
