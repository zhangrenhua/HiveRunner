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
import java.util.Arrays;

@ExtendWith(HiveRunnerExtension.class)
public class MultipleExecutionEnginesTest {

    @HiveSQL(files = {}, autoStart = false)
    public HiveShell shell;


    @Test
    public void test() throws IOException {
        shell.getResourceOutputStream("/tmp/foo/data.txt").write("a,b,c\nd,e,f".getBytes());
        shell.addSetupScript("DROP TABLE IF EXISTS foo");
        shell.addSetupScript(
                "create external table foo (s1 string, s2 string, s3 string) " +
                        "ROW FORMAT DELIMITED " +
                        "FIELDS TERMINATED BY ',' " +
                        "LOCATION '/tmp/foo/'");
        shell.start();

        Assertions.assertEquals(Arrays.asList("a\tb\tc", "d\te\tf"), shell.executeQuery("select * from foo"));

        shell.execute("set hive.tez.container.size=512");
        shell.execute("set hive.execution.engine=tez");
        Assertions.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));

        shell.execute("set hive.execution.engine=mr");
        Assertions.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));

        shell.execute("set hive.execution.engine=tez");
        Assertions.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));

        shell.execute("set hive.execution.engine=mr");
        Assertions.assertEquals(Arrays.asList("2"), shell.executeQuery("select count(1) from foo"));


    }


}
