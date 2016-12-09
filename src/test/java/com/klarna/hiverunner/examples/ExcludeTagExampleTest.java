package com.klarna.hiverunner.examples;

import com.google.common.io.Resources;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Paths;
import java.util.Arrays;


/**
 * Example of how to tag a sql block to be excluded from the test run. This might come in handy
 * when using sql not supported by HiveRunner, E.g: Some optimisation settings or add jar statements.
 */
@RunWith(StandaloneHiveRunner.class)
public class ExcludeTagExampleTest {
    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void excludeFromScript() {
        shell.execute(Paths.get(Resources.getResource("examples/excludeTagExample.sql").getPath()));

        // Verify that foo.bar was not dropped
        Assert.assertEquals(Arrays.asList("bar"), shell.executeQuery("show tables in foo"));
    }

    @Test
    public void excludeExampleInline() {
        shell.execute("" +
                "create database foo;\n" +
                "" +
                "create table foo.bar (s string);\n" +
                "" +
                "-- @EXCLUDE_FROM_TEST:BEGIN\n" +
                "" +
                "drop database foo cascade;\n" +
                "" +
                "-- @EXCLUDE_FROM_TEST:END\n");

        // Verify that foo.bar was not dropped
        Assert.assertEquals(Arrays.asList("bar"), shell.executeQuery("show tables in foo"));
    }

}
