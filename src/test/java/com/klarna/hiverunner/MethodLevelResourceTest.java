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

import com.google.common.io.Resources;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;

@ExtendWith(HiveRunnerExtension.class)
public class MethodLevelResourceTest {

    @HiveSetupScript
    String createTable1 = "DROP TABLE IF EXISTS foo";

    @HiveSetupScript
    String createTable2 = "CREATE EXTERNAL TABLE foo (i INT, j INT, k INT)" +
            "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
            "  STORED AS TEXTFILE" +
            "  LOCATION '/tmp'";

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell hiveShell;

    @Test()
    public void resourceLoadingAsStringTest() {

        hiveShell.addResource("/tmp/data.csv", "1,2,3");
        hiveShell.start();

        Assertions.assertEquals(Arrays.asList("1\t2\t3"), hiveShell.executeQuery("SELECT * FROM foo"));
    }

    @Test()
    public void resourceLoadingAsFileTest() throws URISyntaxException {

        hiveShell.addResource("/tmp/data.csv",
                new File(Resources.getResource("MethodLevelResourceTest/MethodLevelResourceTest.txt").toURI()));

        hiveShell.start();
        Assertions.assertEquals(Arrays.asList("1\t2\t3"), hiveShell.executeQuery("SELECT * FROM foo"));
    }


}
