USE ${hiveconf:my.schema};

DROP TABLE IF EXISTS foo_prim;
CREATE TABLE foo_prim as select i, s from foo;



