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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

@ExtendWith(HiveRunnerExtension.class)
public class OrcSnappyTest {

    @HiveResource(targetFile = "/tmp/foo/data.csv")
    private String data = "A,B\nC,D\nE,F";

    @HiveSQL(files = {"OrcSnappyTest/ctas.sql"})
    private HiveShell hiveShell;

    @Test
    public void tablesShouldBeCreated() {
        List<String> expected = Arrays.asList("foo", "foo_orc_nocomp", "foo_orc_snappy");
        List<String> actual = hiveShell.executeQuery("show tables");
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void verifyThatDataIsAvailableInOrcNocomp() {
        List<String> expected = Arrays.asList("A\tB", "C\tD", "E\tF");
        List<String> actual = hiveShell.executeQuery("select * from foo_orc_nocomp");
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void verifyThatDataIsAvailableInOrcSnappy() {
        List<String> expected = Arrays.asList("A\tB", "C\tD", "E\tF");
        List<String> actual = hiveShell.executeQuery("select * from foo_orc_snappy");
        Assertions.assertEquals(expected, actual);
    }

    @Disabled // Fails with java.lang.UnsatisfiedLinkError: org.apache.hadoop.util.NativeCodeLoader.buildSupportsSnappy()Z
    @Test
    public void testCountOrcNocomp() {
        List<String> expected = Arrays.asList("3");
        List<String> actual = hiveShell.executeQuery("select count(*) from foo_orc_nocomp");
        Assertions.assertEquals(expected, actual);
    }

    @Disabled // Fails with java.lang.UnsatisfiedLinkError: org.apache.hadoop.util.NativeCodeLoader.buildSupportsSnappy()Z
    @Test
    public void testCountOrcSnappy() {
        List<String> expected = Arrays.asList("3");
        List<String> actual = hiveShell.executeQuery("select count(*) from foo_orc_snappy");
        Assertions.assertEquals(expected, actual);
    }

}
