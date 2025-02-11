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

import java.io.File;

@ExtendWith(HiveRunnerExtension.class)
public class UnresolvedResourcePathTest {


    @HiveSQL(files = {}, autoStart = false)
    private HiveShell shell;


    @Test
    public void resourceFileShouldNotBeCreatedIfReferencesAreUnresolved() {
        shell.addResource("${hiveconf:foo}/bar/baz.csv", "A,B,C");
        Assertions.assertThrows(IllegalArgumentException.class, () -> shell.start());
    }

    @Test
    public void resourceFileShouldBeCreatedInsideTempDir() {
        shell.addResource("/tmp/bar/baz.csv", "A,B,C");
        shell.start();
        Assertions.assertTrue(new File(shell.getHiveConf().get("hadoop.tmp.dir"), "bar/baz.csv").exists());
    }

    @Test
    public void resourceFilePathShouldAlwaysBeInsideTempDir() {
        shell.addResource("/bar/baz.csv", "A,B,C");
        Assertions.assertThrows(IllegalArgumentException.class, () -> shell.start());
    }


}
