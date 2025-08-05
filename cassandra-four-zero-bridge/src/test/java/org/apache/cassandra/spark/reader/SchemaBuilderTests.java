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
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(FBUtilities.classForName("org.apache.cassandra.dht.Murmur3Partitioner", "Murmur3Partitioner")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.dht.RandomPartitioner", "RandomPartitioner")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.AbstractCompositeType", "AbstractCompositeType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.AbstractType", "AbstractType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.AsciiType", "AsciiType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.BooleanType", "BooleanType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.BytesType", "BytesType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.ByteType", "ByteType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.CollectionType", "CollectionType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.CompositeType", "CompositeType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.CounterColumnType", "CounterColumnType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.DateType", "DateType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.DecimalType", "DecimalType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.DoubleType", "DoubleType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.DurationType", "DurationType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.DynamicCompositeType", "DynamicCompositeType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.EmptyType", "EmptyType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.FloatType", "FloatType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.FrozenType", "FrozenType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.InetAddressType", "InetAddressType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.Int32Type", "Int32Type")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.IntegerType", "IntegerType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.LexicalUUIDType", "LexicalUUIDType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.ListType", "ListType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.LongType", "LongType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.MapType", "MapType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.NumberType", "NumberType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.PartitionerDefinedOrder", "PartitionerDefinedOrder")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.ReversedType", "ReversedType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.SetType", "SetType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.ShortType", "ShortType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.SimpleDateType", "SimpleDateType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.TemporalType", "TemporalType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.TimestampType", "TimestampType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.TimeType", "TimeType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.TimeUUIDType", "TimeUUIDType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.TupleType", "TupleType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.TypeParser", "TypeParser")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.UserType", "UserType")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.UTF8Type", "UTF8Type")).isNotNull();
        assertThat(FBUtilities.classForName("org.apache.cassandra.db.marshal.UUIDType", "UUIDType")).isNotNull();
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
