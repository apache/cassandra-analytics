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

package org.apache.cassandra.cdc.avro;

import java.util.Collections;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * Defines how to convert Cassandra CQL table schema to equivalent Avro message Schema.
 */
public interface CqlToAvroSchemaConverter
{
    CassandraBridge cassandraBridge();

    default Schema convert(String keyspace, String tableCreateStatement)
    {
        return convert(keyspace, tableCreateStatement, Collections.emptySet());
    }

    default String schemaStringFromCql(String keyspace, String tableCreateStmt)
    {
        return schemaStringFromCql(keyspace, tableCreateStmt, Collections.emptySet());
    }

    default String schemaStringFromCql(String keyspace, String tableCreateStmt, Set<String> udts)
    {
        Schema schema = convert(keyspace, tableCreateStmt, udts);
        return schema.toString(true);
    }

    default void printSchemaFromCql(String keyspace, String tableCreateStmt)
    {
        printSchemaFromCql(keyspace, tableCreateStmt, Collections.emptySet());
    }

    default void printSchemaFromCql(String keyspace, String tableCreateStmt, Set<String> udts)
    {
        System.out.println(schemaStringFromCql(keyspace, tableCreateStmt, udts));
    }

    default Schema convert(String keyspace, String tableCreateStatement, Set<String> udts)
    {
        // To initialize schema in SchemaUtils
        CqlTable cqlTable = cassandraBridge()
                            .buildSchema(tableCreateStatement, keyspace,
                                         new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                               Collections.singletonMap("DC1", 3)),
                                         Partitioner.Murmur3Partitioner,
                                         udts, null, 0, true);

        return convert(cqlTable);
    }

    Schema convert(CqlTable cqlTable);
}
