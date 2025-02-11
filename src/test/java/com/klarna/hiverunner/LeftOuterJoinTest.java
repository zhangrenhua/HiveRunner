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
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

@ExtendWith(HiveRunnerExtension.class)
public class LeftOuterJoinTest {

    private final String hdfsSourceFoo = "/tmp/foo";
    private final String hdfsSourceBar = "/tmp/bar";

    @HiveSetupScript
    String setup =
            "  CREATE TABLE foo (" +
                    " id string," +
                    " value string" +
                    "  )" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '" + hdfsSourceFoo + "' ; "
                    +
                    "  CREATE TABLE bar (" +
                    " id string," +
                    " value string" +
                    "  )" +
                    "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
                    "  STORED AS TEXTFILE" +
                    "  LOCATION '" + hdfsSourceBar + "' ;" +
                    "";


    @HiveSQL(files = {}, autoStart = false)
    private HiveShell hiveShell;


    @Test
    public void leftOuterJoin() {
        hiveShell.addResource(hdfsSourceFoo + "/data.csv",
                "id1\tfoo_value1\nid3\tfoo_value3");
        hiveShell.addResource(hdfsSourceBar + "/data.csv",
                "id1\tbar_value1\n" +
                        "id2\tbar_value2");
        hiveShell.start();

        String query = "SELECT foo.id, bar.value FROM foo left outer join bar on (foo.id = bar.id)";

        List<String> expected = Arrays.asList("id1\tbar_value1", "id3\tNULL");
        List<String> actual = hiveShell.executeQuery(query);

        Assertions.assertEquals(expected, actual);
    }


}
