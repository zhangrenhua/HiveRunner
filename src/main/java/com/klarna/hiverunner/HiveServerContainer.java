/*
 * Copyright 2013 Klarna AB
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

import com.klarna.reflection.ReflectionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.TException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HiveServer wrapper
 */
public class HiveServerContainer {


    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServerContainer.class);

    private static HiveServer.HiveServerHandler client;

    private HiveServerContext context;
    private static Map<String,String> defaultProperties = new HashMap<>();

    HiveServerContainer() {
    }

    public HiveServer.HiveServerHandler getClient() {
        return client;
    }

    /**
     * Will start the HiveServer.
     * @param testConfig Specific test case properties. Will be merged with the HiveConf of the context
     * @param context    The context configuring the HiveServer and it's environment
     */
    public void init(Map<String, String> testConfig, HiveServerContext context) {

        this.context = context;

        HiveConf hiveConf = context.getHiveConf();

        if (client == null) {
            try {
                client = new HiveServer.HiveServerHandler(hiveConf);
            } catch (MetaException e) {
                throw new IllegalStateException("Failed to create HiveServer :" + e.getMessage(), e);
            }
        }

        cleanAndSetTestCaseProperties(testConfig, hiveConf);
    }

    private void cleanAndSetTestCaseProperties(Map<String, String> testConfig, HiveConf hiveConf) {
        for (Map.Entry<String, String> defaultProperty : defaultProperties.entrySet()) {
            if (defaultProperty.getValue() != null) {
                setProperty(defaultProperty.getKey(), defaultProperty.getValue());
            }
        }

        defaultProperties = new HashMap<String, String>();

        for (Map.Entry<String, String> valuePair : testConfig.entrySet()) {
            defaultProperties.put(valuePair.getKey(), hiveConf.get(valuePair.getKey()));
            setProperty(valuePair.getKey(), valuePair.getValue());
        }
    }

    private void setProperty(String key, String value) {
        try {
            client.execute("set " + key + "=" + value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set property: " + e.getMessage(), e);
        }
    }


    public TemporaryFolder getBaseDir() {
        return context.getBaseDir();
    }


    /**
     * Executes a single hql statement.
     * @param hiveql to execute
     * @return the result of the statement
     */
    public List<String> executeQuery(String hiveql) {
        try {
            client.execute(hiveql);
            return client.fetchAll();
        } catch (TException e) {
            throw new IllegalStateException("Failed to executeQuery Hive query " + hiveql + ": " + e.getMessage(), e);
        }
    }

    /**
     * Executes a hive script.
     * @param hiveql hive script statements.
     */
    public void executeScript(String hiveql) {
        for (String statement : splitStatements(hiveql)) {
            try {
                client.execute(statement);
            } catch (TException e) {
                throw new IllegalStateException(
                        "Failed to executeQuery Hive query " + statement + ": " + e.getMessage(), e);
            }
        }

    }

    /**
     * Release all resources.
     */
    public void tearDown() {
        try {

            // Drop all hive databases except default (cannot be dropped)
            List<String> databases = executeQuery("show databases");
            for (String database : databases) {
                if (!database.equals("default")) {
                    executeQuery("drop database " + database + " CASCADE");
                }
            }

            // Drop all tables in default database
            executeScript("USE default");
            List<String> tables = executeQuery("show tables");
            for (String table : tables) {
                executeScript("drop table " + table);
            }
        } catch (Throwable e) {
            throw new IllegalStateException("Failed to reset to default schema: " + e.getMessage(), e);
        } finally {
            client.shutdown();
            client = null;

            ReflectionUtils.setStaticField(HiveMetaStore.HMSHandler.class, "createDefaultDB", false);

            LOGGER.info("Tore down HiveServer instance");
        }
    }

    public String expandVariableSubstitutes(String expression) {
        return new VariableSubstitution().substitute(getClient().getHiveConf(), expression);
    }

    /**
     * Package protected due to testability convenience.
     */
    String[] splitStatements(String hiveql) {
        return hiveql.split("(?<=[^\\\\]);");
    }

    private void pingHiveServer() {
        // Ping hive server before we do anything more with it! If validation
        // is switched on, this will fail if metastorage is not set up properly
        try {
            client.execute("SHOW TABLES");
        } catch (TException e) {
            throw new IllegalStateException("Failed to ping HiveServer: " + e.getMessage(), e);
        }
    }


}

