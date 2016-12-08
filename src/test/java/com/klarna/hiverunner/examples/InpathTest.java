package com.klarna.hiverunner.examples;


import com.google.common.io.Resources;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class InpathTest {

    @HiveSQL(files = {}, autoStart = true)
    private HiveShell shell;


    @Test
    public void setInpath() {

        String indataClassPathRef = "examples/inpathtest/local.data.csv";

        shell.execute("set db.name=foo_db");
        shell.execute("set local.data.inpath=" +  Resources.getResource(indataClassPathRef).getPath());

        shell.execute("create database ${hiveconf:db.name}");
        shell.execute("create table ${hiveconf:db.name}.bar (s string)");

        String query = "LOAD DATA LOCAL INPATH '${hiveconf:local.data.inpath}' OVERWRITE INTO TABLE ${hiveconf:db.name}.bar";

        shell.execute(query);

        List<String> expected = Arrays.asList("foo", "bar", "baz");
        List<String> actual = shell.executeQuery("select * from ${hiveconf:db.name}.bar");

        Assert.assertEquals(expected, actual);
    }

}
