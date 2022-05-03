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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.klarna.hiverunner.builder.Statement;
import com.klarna.hiverunner.io.IgnoreClosePrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HiveServer wrapper
 */
public class HiveServerContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServerContainer.class);

    private final HiveServerContext context;

    private Map<String, String> hiveVariables;

    private Connection connection;

    public HiveServerContainer(HiveServerContext context) {
        this.context = context;
    }

    /**
     * Will start the HiveServer.
     *
     * @param testConfig Specific test case properties. Will be merged with the HiveConf of the context
     * @param hiveVars   HiveVars to pass on to the HiveServer for this session
     */
    public void init(Map<String, String> testConfig, Map<String, String> hiveVars) {
        hiveVariables = hiveVars;

        try {
            connection = DriverManager.getConnection("jdbc:hive2://192.168.10.211:12345/default", "noUser", "noPassword");


            // merge test case properties with hive conf before HiveServer is started.
            for (Map.Entry<String, String> property : testConfig.entrySet()) {
                try (final java.sql.Statement statement = connection.createStatement()) {
                    statement.execute("set " + property.getKey() + "=" + property.getValue());
                }
            }
            Preconditions.checkNotNull(connection, "ClIService was not initialized by HiveServer2");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create HiveServer :" + e.getMessage(), e);
        }

        // Ping hive server before we do anything more with it! If validation
        // is switched on, this will fail if metastorage is not set up properly
        pingHiveServer();
    }

    public Path getBaseDir() {
        return context.getBaseDir();
    }

    public List<Object[]> executeStatement(Statement hiveql) {
        return executeStatement(hiveql.getSql());
    }

    public List<Object[]> executeStatement(String hiveql) {
        // This PrintStream hack can be removed if/when IntelliJ fixes https://youtrack.jetbrains.com/issue/IDEA-120628
        // See https://github.com/klarna/HiveRunner/issues/94 for more info.
        PrintStream initialPrintStream = System.out;
        System.setOut(new IgnoreClosePrintStream(System.out));

        try (final java.sql.Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(hiveql)) {
            List<Object[]> result = new ArrayList<>();
            final int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = resultSet.getObject(i + 1);
                }
                result.add(row);
            }

            LOGGER.debug("ResultSet:\n" + Joiner.on("\n").join(Iterables.transform(result, new Function<Object[], String>() {
                @Nullable
                @Override
                public String apply(@Nullable Object[] objects) {
                    return Joiner.on(", ").useForNull("null").join(objects);
                }
            })));

            return result;
        } catch (SQLException e) {
            throw new IllegalArgumentException("Failed to executeQuery Hive query " + hiveql + ": " + e.getMessage(), e);
        } finally {
            System.setOut(initialPrintStream);
        }
    }

    /**
     * Release all resources.
     * <p>
     * This call will never throw an exception as it makes no sense doing that in the tear down phase.
     * </p>
     */
    public void tearDown() {

        try {
            TezJobExecHelper.killRunningJobs();
        } catch (Throwable e) {
            LOGGER.warn("Failed to kill tez session: " + e.getMessage() + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        try {
            // Reset to default schema
            executeStatement("USE default");
        } catch (Throwable e) {
            LOGGER.warn("Failed to reset to default schema: " + e.getMessage() + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        try {
            connection.close();
        } catch (Throwable e) {
            LOGGER.warn("Failed to close client session: " + e.getMessage() + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        connection = null;

        LOGGER.info("Tore down HiveServer instance");
    }

    public String expandVariableSubstitutes(String expression) {
        return getVariableSubstitution().substitute(getHiveConf(), expression);
    }

    private void pingHiveServer() {
        executeStatement("SHOW TABLES");
    }

    public HiveConf getHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", "thrift://192.168.10.211:9083");
        //当前版本2.3.4与集群3.0版本不兼容，加入此设置
//        hiveMetaStoreClient.setMetaConf("hive.metastore.client.capability.check", "false");
        return hiveConf;
    }

    public VariableSubstitution getVariableSubstitution() {
        return new VariableSubstitution(new HiveVariableSource() {
            @Override
            public Map<String, String> getHiveVariable() {
                return hiveVariables;
            }
        });
    }

    public Connection getConnection() {
        return connection;
    }

}
