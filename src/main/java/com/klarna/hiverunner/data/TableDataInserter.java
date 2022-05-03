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
package com.klarna.hiverunner.data;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.klarna.hiverunner.HiveServerContainer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

class TableDataInserter {

    private final HiveServerContainer hiveServerContainer;

    private final String databaseName;
    private final String tableName;
    private final Map<String, String> config;

    TableDataInserter(String databaseName, String tableName, HiveConf conf, HiveServerContainer hiveServerContainer) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        config = Maps.fromProperties(conf.getAllProperties());
        this.hiveServerContainer = hiveServerContainer;
    }

    void insert(Multimap<Map<String, String>, HCatRecord> data) {
        Iterator<Map<String, String>> iterator = data.keySet().iterator();
        while (iterator.hasNext()) {
            Map<String, String> partitionSpec = iterator.next();
            insert(partitionSpec, data.get(partitionSpec));
        }
    }

    private void insert(Map<String, String> partitionSpec, Iterable<HCatRecord> rows) {
        WriteEntity entity = new WriteEntity.Builder()
                .withDatabase(databaseName)
                .withTable(tableName)
                .withPartition(partitionSpec)
                .build();

        try {
            for (HCatRecord row : rows) {
                StringBuilder insertBuilder = new StringBuilder("insert into ");
                if (StringUtils.isNoneBlank(databaseName)) {
                    insertBuilder.append(databaseName).append(".");
                }
                insertBuilder.append(tableName);
                insertBuilder.append(" values(");

                for (Object o : row.getAll()) {
                    String value;
                    if (o == null) {
                        value = null;
                    } else if (o instanceof String || o instanceof Character) {
                        value = "'" + o + "'";
                    } else if (o instanceof java.util.Date) {
                        value = "'" + DateFormatUtils.format((Date) o, "yyyy-MM-dd HH:mm:ss") + "'";
                    } else {
                        value = String.valueOf(o);
                    }
                    insertBuilder.append(value).append(",");
                }
                insertBuilder.delete(insertBuilder.length() - 1, insertBuilder.length());
                insertBuilder.append(")");

                hiveServerContainer.executeStatement(insertBuilder.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException("An error occurred while inserting data to " + databaseName + "." + tableName, e);
        }
    }
}
