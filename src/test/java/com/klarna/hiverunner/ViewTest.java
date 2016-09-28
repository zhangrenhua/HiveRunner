package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class ViewTest {
    @HiveSQL(files = {})
    protected HiveShell shell;

    @Test
    public void createView() {
        shell.execute("create database test_db");

        shell.execute(new StringBuilder()
                .append("create table test_db.tableA (")
                .append("id int, ")
                .append("value string")
                .append(")")
                .toString());

        shell.execute(new StringBuilder()
                .append("create table test_db.tableB (")
                .append("id int, ")
                .append("value string")
                .append(")")
                .toString());

        shell.insertInto("test_db", "tableA")
                .addRow(1, "v1")
                .addRow(2, "v2")
                .commit();
        shell.insertInto("test_db", "tableB")
                .addRow(1, "v3")
                .addRow(2, "v4")
                .commit();

        shell.execute(new StringBuilder()
                .append("create view test_db.test_view ")
                .append("as select a.* from test_db.tableA a, test_db.tableB b  ")
                .append("where a.id = b.id")
                .toString());

        shell.executeStatement("select * from test_db.test_view");
    }


}
