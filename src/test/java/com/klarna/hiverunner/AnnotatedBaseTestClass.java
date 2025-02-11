/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HiveRunnerExtension.class)
public abstract class AnnotatedBaseTestClass {
    @HiveSQL(files = {})
    protected HiveShell shell;

    @BeforeEach
    public void setup() {
        shell.execute("DROP DATABASE IF EXISTS test_db");
        shell.execute("create database test_db");

        shell.execute("DROP TABLE IF EXISTS test_db.test_table");
        shell.execute(new StringBuilder()
                .append("create table test_db.test_table (")
                .append("c0 string")
                .append(")")
                .toString());

        shell.insertInto("test_db", "test_table")
                .addRow("v1")
                .commit();
    }
}
