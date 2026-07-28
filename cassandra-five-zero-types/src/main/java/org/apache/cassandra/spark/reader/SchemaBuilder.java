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

package org.apache.cassandra.spark.reader;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.Nullable;

public class SchemaBuilder extends AbstractSchemaBuilder
{
    public SchemaBuilder(CqlTable table, Partitioner partitioner, boolean enableCdc)
    {
        this(table, partitioner, null, enableCdc);
    }

    public SchemaBuilder(CqlTable table, Partitioner partitioner)
    {
        this(table, partitioner, null, false);
    }

    public SchemaBuilder(CqlTable table, Partitioner partitioner, UUID tableId, boolean enableCdc)
    {
        this(table.createStatement(),
             table.keyspace(),
             table.replicationFactor(),
             partitioner,
             table::udtCreateStmts,
             tableId,
             table.indexStatements(),
             enableCdc);
    }

    @VisibleForTesting
    public SchemaBuilder(String createStmt, String keyspace, ReplicationFactor replicationFactor)
    {
        this(createStmt, keyspace, replicationFactor, Partitioner.Murmur3Partitioner, bridge -> Collections.emptySet(),
             null, Collections.emptySet(), false);
    }

    @VisibleForTesting
    public SchemaBuilder(String createStmt,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Partitioner partitioner)
    {
        this(createStmt, keyspace, replicationFactor, partitioner, bridge -> Collections.emptySet(), null,
             Collections.emptySet(), false);
    }

    public SchemaBuilder(String createStmt,
                         String keyspace,
                         ReplicationFactor replicationFactor,
                         Partitioner partitioner,
                         Function<CassandraTypes, Set<String>> udtStatementsProvider,
                         @Nullable UUID tableId,
                         Set<String> indexStatements,
                         boolean enableCdc)
    {
        super(createStmt, keyspace, replicationFactor, partitioner, udtStatementsProvider,
              tableId, indexStatements, enableCdc);
    }

    @Override
    protected void validateType(CQL3Type cqlType)
    {
        if (!(cqlType instanceof CQL3Type.Native)
            && !(cqlType instanceof CQL3Type.Collection)
            && !(cqlType instanceof CQL3Type.UserDefined)
            && !(cqlType instanceof CQL3Type.Tuple)
            && !(cqlType instanceof CQL3Type.Vector))
        {
            throw new UnsupportedOperationException("Only native, collection, tuples, vectors or UDT data types are supported, "
                                                    + "unsupported data type: " + cqlType.toString());
        }
        if (cqlType instanceof CQL3Type.Vector)
        {
            CQL3Type.Vector vector = (CQL3Type.Vector) cqlType;
            VectorType<?> vectorType = vector.getType();
            for (AbstractType<?> subType : vectorType.subTypes())
            {
                validateType(subType);
            }
            return;
        }
        super.validateType(cqlType);
    }
}
