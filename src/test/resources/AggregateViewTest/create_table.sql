CREATE DATABASE db;

DROP TABLE IF EXISTS db.mvtdescriptionchangeinfo;
CREATE TABLE db.mvtdescriptionchangeinfo(
  timestamp bigint COMMENT '',
  testid string COMMENT '',
  type string COMMENT '',
  contents string COMMENT '',
  hostname string COMMENT '')
PARTITIONED BY (
  request_log_date string,
  request_log_hour string);

  DROP TABLE IF EXISTS db.latestnodemvtchanges;
CREATE VIEW db.latestnodemvtchanges AS
  SELECT testid, hostname, max(timestamp) AS mts
  FROM db.mvtdescriptionchangeinfo
  WHERE timestamp IS NOT NULL
  GROUP BY testid, hostname;

  DROP TABLE IF EXISTS db.latesttestchangepairs;
CREATE VIEW db.latesttestchangepairs AS
  SELECT a.testid, a.type
  FROM db.mvtdescriptionchangeinfo a
  INNER JOIN db.latestnodemvtchanges b ON a.testid = b.testid AND a.timestamp = b.mts
  GROUP BY a.testid, a.type;
