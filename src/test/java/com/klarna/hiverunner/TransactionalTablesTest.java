package com.klarna.hiverunner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public class TransactionalTablesTest {
  
  @HiveSQL(files = {"TransactionalTablesTest/create_table.sql"}, autoStart = false)
  private HiveShell shell;
  
  @Test
  public void createTransactionalTable() {
    shell.setHiveConfValue("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    shell.setHiveConfValue("hive.support.concurrency", "true");
    shell.setHiveConfValue("hive.enforce.bucketing", "true");
    shell.setHiveConfValue("hive.exec.dynamic.partition.mode", "nostrict");
    shell.setHiveConfValue("hive.compactor.initiator.on", "true");
    shell.setHiveConfValue("hive.compactor.worker.threads", "1");
    shell.start();
    
    List<String> actual = shell.executeQuery("select * from transaction_test");
    assertEquals(0, actual.size());
  }

}
