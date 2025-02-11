/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2022 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.commons.collections.MapUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@ExtendWith(HiveRunnerExtension.class)
public class PartitionSupportTest {

    private final String tableName = "foo_bar";

    @HiveResource(targetFile = "${hiveconf:HDFS_ROOT_FOO}/foo/year=2013/month=11/data.csv")
    public String data1 = "a,b,c\nf,g,h\nt,j,k";

    @HiveResource(targetFile = "${hiveconf:HDFS_ROOT_FOO}/foo/year=2012/month=02/data.csv")
    public String data2 = "q,w,e\nr,t,y\nu,i,o";


    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new String[]{
            "table.name", tableName,
            "HDFS_ROOT_FOO", "/tmp"
    });

    @HiveSQL(files = "PartitionSupportTest/hql_example.sql")
    public HiveShell hiveShell;


    @BeforeEach
    public void repairPartitions() {
        // TODO: Incorporate support for REPAIR TABLE in HiveRunner fwk.
        // if new partitions are directly added to HDFS the metastore is not aware of these partitions.
        // 'MSCK REPAIR TABLE table' adds metadata about partitions to the Hive metastore for partitions
        // for which such metadata doesn't already exist.
        hiveShell.execute("MSCK REPAIR TABLE ${hiveconf:table.name}");
    }


    @Test
    public void testSelectMax() throws TException, IOException {
        Assertions.assertEquals(
                Arrays.asList("11"),
                hiveShell.executeQuery(String.format("select max(month) from %s", tableName)));

        Assertions.assertEquals(
                Arrays.asList("2"),
                hiveShell.executeQuery(String.format("select min(month) from %s", tableName)));
    }

    @Test
    public void testShowTables() {
        Assertions.assertEquals(Arrays.asList(tableName), hiveShell.executeQuery("SHOW TABLES"));
    }


}
