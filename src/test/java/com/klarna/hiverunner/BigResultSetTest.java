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

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ExtendWith(HiveRunnerExtension.class)
public class BigResultSetTest {

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell hiveShell;


    /**
     * This test verifies that we can fetch more than 100 rows of data from hive.
     * This test was added due to tests failing with result sets bigger than 100 rows.
     */
    @Test
    public void bigResultSetTest() throws IOException {
        hiveShell.setHiveConfValue("location", "/tmp/foo");
        hiveShell.addSetupScript("DROP TABLE IF EXISTS foo");
        hiveShell.addSetupScript("CREATE table FOO (s String) LOCATION '${hiveconf:location}'");
        OutputStream ros = hiveShell.getResourceOutputStream("${hiveconf:location}/foo.data");

        List<String> rows = new ArrayList<>();

        for (int i = 0; i < 1099; i++) {
            String row = UUID.randomUUID().toString();
            rows.add(row);
            ros.write((row + "\n").getBytes());
        }

        hiveShell.start();

        Assertions.assertEquals(rows, hiveShell.executeQuery("select * from FOO"));

    }
}
