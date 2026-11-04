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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.PartitionKeyEncoder;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;

/**
 * Computes the {@link DecoratedKey} of each row of a bulk write, which Spark uses to partition and sort the rows
 * before they are written to SSTables.
 * <p>
 * The partition key is serialized by the Cassandra bridge, with the {@link PartitionKeyEncoder} that matches the
 * bridge's SSTable writer. Every partition key type that Cassandra allows is supported, and the token computed for
 * a row is the token of the partition that the SSTable writer writes the row to.
 * <p>
 * Spark serializes the tokenizer to the executors, once for each task. The bridge objects it uses are not
 * serializable, so they are built the first time a JVM keys a row of the table, and shared by the tokenizers
 * of all the tasks that the JVM runs for the table.
 */
public class Tokenizer implements Serializable
{
    private static final long serialVersionUID = 6446524718233491563L;
    // Building a key encoder updates the schema of the bridge, under a lock that is global to the JVM
    private static final Map<List<Object>, KeyEncoder> KEY_ENCODERS = new ConcurrentHashMap<>();

    interface SerializableFunction<T, R> extends Function<T, R>, Serializable
    {
    }

    private final SerializableFunction<Object[], Object[]> keyColumnProvider;
    private final CassandraVersion bridgeVersion;
    private final String keyspace;
    private final String createStatement;
    private final Set<String> userDefinedTypeStatements;
    private final Partitioner partitioner;
    private transient volatile KeyEncoder keyEncoder;

    /**
     * @param schemaInfo  schema of the table being written
     * @param keyspace    keyspace of the table being written
     * @param partitioner partitioner of the cluster being written to
     */
    public Tokenizer(@NotNull BroadcastableSchemaInfo schemaInfo, @NotNull String keyspace, @NotNull Partitioner partitioner)
    {
        this(schemaInfo.getBroadcastableTableSchema()::getKeyColumns,
             schemaInfo.getBroadcastableTableSchema().getBridgeVersion(),
             keyspace,
             schemaInfo.getBroadcastableTableSchema().getCreateStatement(),
             schemaInfo.getUserDefinedTypeStatements(),
             partitioner);
    }

    /**
     * @param keyColumnIndexes          positions in a row of the primary key columns, starting with the partition
     *                                  key columns in partition key order
     * @param bridgeVersion             version of the Cassandra bridge to use
     * @param keyspace                  keyspace of the table being written
     * @param createStatement           CQL statement that creates the table being written
     * @param userDefinedTypeStatements CQL statements that create the user-defined types the table uses
     * @param partitioner               partitioner of the cluster being written to
     */
    @VisibleForTesting
    public Tokenizer(@NotNull List<Integer> keyColumnIndexes,
                     @NotNull CassandraVersion bridgeVersion,
                     @NotNull String keyspace,
                     @NotNull String createStatement,
                     @NotNull Set<String> userDefinedTypeStatements,
                     @NotNull Partitioner partitioner)
    {
        this(columns -> BroadcastableTableSchema.getKeyColumns(columns, keyColumnIndexes),
             bridgeVersion,
             keyspace,
             createStatement,
             userDefinedTypeStatements,
             partitioner);
    }

    private Tokenizer(SerializableFunction<Object[], Object[]> keyColumnProvider,
                      CassandraVersion bridgeVersion,
                      String keyspace,
                      String createStatement,
                      Set<String> userDefinedTypeStatements,
                      Partitioner partitioner)
    {
        this.keyColumnProvider = keyColumnProvider;
        this.bridgeVersion = bridgeVersion;
        this.keyspace = keyspace;
        this.createStatement = createStatement;
        this.userDefinedTypeStatements = userDefinedTypeStatements;
        this.partitioner = partitioner;
    }

    /**
     * @param columns a row normalized by {@link BroadcastableTableSchema#normalize}; it is not modified
     * @return the token and the serialized partition key of the row
     */
    public DecoratedKey getDecoratedKey(Object[] columns)
    {
        Object[] keyColumns = keyColumnProvider.apply(columns);
        ByteBuffer key = keyEncoder().encode(keyColumns);
        return new DecoratedKey(partitioner.hash(key), key);
    }

    private KeyEncoder keyEncoder()
    {
        KeyEncoder encoder = keyEncoder;
        if (encoder == null)
        {
            List<Object> key = Arrays.asList(bridgeVersion, keyspace, createStatement, userDefinedTypeStatements, partitioner);
            encoder = KEY_ENCODERS.computeIfAbsent(key, ignored -> new KeyEncoder(CassandraBridgeFactory.get(bridgeVersion),
                                                                                  keyspace,
                                                                                  createStatement,
                                                                                  userDefinedTypeStatements,
                                                                                  partitioner));
            // Threads that race here store the same shared instance
            keyEncoder = encoder;
        }
        return encoder;
    }

    /**
     * Converts the partition key values of a row in the same way as {@link RecordWriter} before serializing them,
     * so that the serialized key is the one the SSTable writer derives from the converted row.
     * <p>
     * Thread-safe, as the task threads of a JVM share it.
     */
    private static final class KeyEncoder
    {
        private final List<String> partitionKeyColumnNames;
        private final CqlWriterValueConverter valueConverter;
        private final PartitionKeyEncoder partitionKeyEncoder;

        KeyEncoder(CassandraBridge bridge,
                   String keyspace,
                   String createStatement,
                   Set<String> userDefinedTypeStatements,
                   Partitioner partitioner)
        {
            CqlTable cqlTable = bridge.buildSchema(createStatement,
                                                   keyspace,
                                                   RecordWriter.IGNORED_REPLICATION_FACTOR,
                                                   partitioner,
                                                   userDefinedTypeStatements);
            this.partitionKeyColumnNames = cqlTable.partitionKeys()
                                                   .stream()
                                                   .map(CqlField::name)
                                                   .collect(Collectors.toList());
            this.valueConverter = new CqlWriterValueConverter(cqlTable, bridge, partitioner);
            this.partitionKeyEncoder = bridge.partitionKeyEncoder(cqlTable, partitioner);
        }

        ByteBuffer encode(Object[] keyColumns)
        {
            List<Object> partitionKeyValues = new ArrayList<>(partitionKeyColumnNames.size());
            for (int index = 0; index < partitionKeyColumnNames.size(); index++)
            {
                String columnName = partitionKeyColumnNames.get(index);
                partitionKeyValues.add(valueConverter.convert(columnName, keyColumns[index]));
            }
            return partitionKeyEncoder.encode(partitionKeyValues);
        }
    }
}
