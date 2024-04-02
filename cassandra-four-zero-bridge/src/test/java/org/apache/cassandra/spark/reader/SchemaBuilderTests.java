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

import java.util.HashMap;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.spark.reader.SchemaBuilder.rfToMap;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SchemaBuilderTests
{
    @Test
    public void getCompactionClass()
    {
        FBUtilities.classForName("org.apache.cassandra.db.compaction.LeveledCompactionStrategy", "LeveledCompactionStrategy");
    }

    @Test
    public void testDataTypes()
    {
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.dht.Murmur3Partitioner", "Murmur3Partitioner"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.dht.RandomPartitioner", "RandomPartitioner"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.AbstractCompositeType", "AbstractCompositeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.AbstractType", "AbstractType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.AsciiType", "AsciiType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.BooleanType", "BooleanType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.BytesType", "BytesType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.ByteType", "ByteType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.CollectionType", "CollectionType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.CompositeType", "CompositeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.CounterColumnType", "CounterColumnType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.DateType", "DateType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.DecimalType", "DecimalType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.DoubleType", "DoubleType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.DurationType", "DurationType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.DynamicCompositeType", "DynamicCompositeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.EmptyType", "EmptyType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.FloatType", "FloatType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.FrozenType", "FrozenType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.InetAddressType", "InetAddressType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.Int32Type", "Int32Type"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.IntegerType", "IntegerType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.LexicalUUIDType", "LexicalUUIDType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.ListType", "ListType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.LongType", "LongType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.MapType", "MapType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.NumberType", "NumberType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.PartitionerDefinedOrder", "PartitionerDefinedOrder"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.ReversedType", "ReversedType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.SetType", "SetType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.ShortType", "ShortType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.SimpleDateType", "SimpleDateType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.TemporalType", "TemporalType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.TimestampType", "TimestampType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.TimeType", "TimeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.TimeUUIDType", "TimeUUIDType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.TupleType", "TupleType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.TypeParser", "TypeParser"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.UserType", "UserType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.UTF8Type", "UTF8Type"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.db.marshal.UUIDType", "UUIDType"));
    }

    @Test
    public void testSchemaBuilderWithPartiallyInitializedMetadata()
    {
        CassandraBridgeImplementation.setup();
        String keyspaceName = "foo" + getClass().getSimpleName();
        ReplicationFactor replicationFactor = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.LocalStrategy, new HashMap<>());
        KeyspaceMetadata keyspaceMetadata = KeyspaceMetadata.create(keyspaceName, KeyspaceParams.create(true, rfToMap(replicationFactor)));
        Schema.instance.load(keyspaceMetadata);
        Keyspace.openWithoutSSTables(keyspaceName);

        String createTableStatement = "CREATE TABLE " + keyspaceName + ".bar (a int PRIMARY KEY)";
        TableMetadata tableMetadata = CQLFragmentParser
                .parseAny(CqlParser::createTableStatement, createTableStatement, "CREATE TABLE")
                .keyspace(keyspaceName)
                .prepare(null)
                .builder(Types.none())
                .build();
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        Schema.instance.load(keyspace.withSwapped(keyspace.tables.with(tableMetadata)));

        new SchemaBuilder(createTableStatement, keyspaceName, replicationFactor);
    }
}
