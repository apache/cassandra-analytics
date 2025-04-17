/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.cdc;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.cassandra.spark.data.CqlField;
import org.jetbrains.annotations.Nullable;

/**
 * Allows CDC to be configured with different logging levels (MINIMAL, PARTITION_KEYS and FULL) to optionally log
 * partition key values or the full row for additional debugging.
 */
public enum CdcLogMode implements CdcLogger
{
    // The default - only logs the keyspace name, table name and event kind
    MINIMAL
    {
        @Override
        public void info(Logger logger, String message, CdcEvent event, String topic)
        {
            logger.info("{}. kind={} keyspace={} table={} topic={}",
                        message, event.getKind(), event.keyspace, event.table, topic);
        }

        @Override
        public void warn(Logger logger, String message, CdcEvent event, String topic,
                         Throwable cause)
        {
            logger.warn("{}. kind={} keyspace={} table={} topic={}",
                        message, event.getKind(), event.keyspace, event.table, topic, cause);
        }

        @Override
        public void error(Logger logger, String message, CdcEvent event, String topic,
                          Throwable cause)
        {
            logger.error("{}. kind={} keyspace={} table={} topic={}",
                         message, event.getKind(), event.keyspace, event.table, topic, cause);
        }
    },

    // In addition to MINIMAL, it logs the partition key values of cdc events
    // Note that it requires column value deserialization and could cause performance hit.
    PARTITION_KEYS
    {
        @Override
        public void info(Logger logger, String message, CdcEvent event, String topic)
        {
            logger.info("{}. kind={} keyspace={} table={} partitionKeys={} topic={}",
                        message, event.getKind(), event.keyspace, event.table,
                        columnsToString(event.getPartitionKeys()), topic);
        }

        @Override
        public void warn(Logger logger, String message, CdcEvent event, String topic, Throwable cause)
        {
            logger.warn("{}. kind={} keyspace={} table={} partitionKeys={} topic={}",
                        message, event.getKind(), event.keyspace, event.table,
                        columnsToString(event.getPartitionKeys()), topic, cause);
        }

        @Override
        public void error(Logger logger, String message, CdcEvent event, String topic, Throwable cause)
        {
            logger.error("{}. kind={} keyspace={} table={} partitionKeys={} topic={}",
                         message, event.getKind(), event.keyspace, event.table,
                         columnsToString(event.getPartitionKeys()), topic, cause);
        }
    },

    // In addition to MINIMAL, it logs all the column values of cdc events
    // Note that it requires column value deserialization and could cause performance hit.
    FULL
    {
        @Override
        public void info(Logger logger, String message, CdcEvent event, String topic)
        {
            logger.info("{}. kind={} keyspace={} table={} partitionKeys={} clusteringKeys={} valueColumns={} topic={}",
                        message, event.getKind(), event.keyspace, event.table,
            columnsToString(event.getPartitionKeys()), columnsToString(event.getClusteringKeys()),
            columnsToString(event.getValueColumns()), topic);
        }

        @Override
        public void warn(Logger logger, String message, CdcEvent event, String topic, Throwable cause)
        {
            logger.warn("{}. kind={} keyspace={} table={} partitionKeys={} clusteringKeys={} valueColumns={} topic={}",
                        message, event.getKind(), event.keyspace, event.table,
            columnsToString(event.getPartitionKeys()), columnsToString(event.getClusteringKeys()),
            columnsToString(event.getValueColumns()), topic, cause);
        }

        @Override
        public void error(Logger logger, String message, CdcEvent event, String topic, Throwable cause)
        {
            logger.error("{}. kind={} keyspace={} table={} partitionKeys={} clusteringKeys={} valueColumns={} topic={}",
                         message, event.getKind(), event.keyspace, event.table,
            columnsToString(event.getPartitionKeys()), columnsToString(event.getClusteringKeys()),
            columnsToString(event.getValueColumns()), topic, cause);
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(CdcLogMode.class);
    private static Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup = null;

    public static synchronized void init(Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup)
    {
        if (CdcLogMode.typeLookup != null)
        {
            LOGGER.warn("CdcLogMode is already initialized!");
            return;
        }
        CdcLogMode.typeLookup = typeLookup;
    }

    public static CdcLogMode fromOption(String name)
    {
        try
        {
            return CdcLogMode.valueOf(name.toUpperCase());
        }
        catch (Exception exception)
        {
            LOGGER.warn("Unrecognized mode: {} from input. Using the default MINIMAL", name,
                        exception);
            return MINIMAL;
        }
    }

    private static String columnToString(Value fieldValue)
    {
        try
        {
            CqlField.CqlType type = typeLookup.apply(KeyspaceTypeKey.of(fieldValue.keyspace, fieldValue.columnType));
            Object javaValue = type.deserializeToJavaType(fieldValue.getValue());
            return String.format("[%s : %s]", fieldValue.columnName, javaValue);
        }
        catch (Throwable t)
        {
            LOGGER.error("Failed to deserialize column value. columnName={}", fieldValue.columnName, t);
            return String.format("[%s : unknown]", fieldValue.columnName);
        }
    }

    private static String columnsToString(@Nullable List<Value> columns)
    {
        return columns == null
               ? "null"
               : columns.stream().map(CdcLogMode::columnToString).collect(Collectors.joining(", "));
    }
}
