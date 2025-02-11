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

import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

@ExtendWith(HiveRunnerExtension.class)
public class IntegerPartitionFormatTest {


    @HiveSQL(files = {})
    public HiveShell hiveShell;

    @HiveResource(targetFile = "/tmp/foo/month=07/foo.data")
    public String data = "06\n6";

    @HiveSetupScript
    public String setup1 = "DROP TABLE IF EXISTS foo;";

    @HiveSetupScript
    public String setup2 =
            "CREATE EXTERNAL TABLE foo (id int)" +
                    "  PARTITIONED BY(month int)" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '/tmp/foo';";

    @BeforeEach
    public void repair() {
        // MSCK REPAIR TABLE adds metadata about partitions to the Hive metastore for
        // partitions for which such metadata doesn't already exist
        hiveShell.execute("set hive.mv.files.thread=0");
        hiveShell.execute("MSCK REPAIR TABLE foo");
    }

    @Test
    public void testInteger() {
        Assertions.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 6"));
    }

    @Test
    public void testPrefixedInteger() {
        Assertions.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 06"));
    }


    @Test
    public void testPrefixedPartitionInteger() {
        Assertions.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 6 and month = 07"));
    }


    @Test
    public void testNonPrefixedPartitionInteger() {
        Assertions.assertEquals(Arrays.asList("6\t7", "6\t7"), hiveShell.executeQuery("select * from foo where id = 6 and month = 7"));
    }
}
