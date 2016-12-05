package com.klarna.hiverunner.examples;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URISyntaxException;
import java.util.HashSet;

@RunWith(StandaloneHiveRunner.class)
public class InsertIntoPartitionTest {

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testInsertIntoPartition() throws URISyntaxException {
        shell.executeQuery("" +
                "CREATE TABLE foo (s1 string) " +
                "PARTITIONED BY (year int, month int)");

        File input = new File(Resources.getResource("examples/insertintopartition/partitioned.csv").toURI());
        shell.insertInto("default", "foo")
                .addRow("A", 1, 2)
                .addRowsFromDelimited(input, ",", null)
                .commit();

        HashSet<String> expected = Sets.newHashSet(
                "A\t1\t2",
                "A\t1\t2",
                "B\t1\t2",
                "C\t11\t12",
                "D\t13\t14");

        HashSet<String> actual = Sets.newHashSet(shell.executeQuery("select * from foo"));

        Assert.assertEquals(expected, actual);
    }
}
