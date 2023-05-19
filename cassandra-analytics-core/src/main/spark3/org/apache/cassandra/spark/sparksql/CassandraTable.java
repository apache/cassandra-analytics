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

package org.apache.cassandra.spark.sparksql;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class CassandraTable implements Table, SupportsRead
{
    private final DataLayer dataLayer;
    private final StructType schema;

    CassandraTable(DataLayer dataLayer, StructType schema)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
    }

    @Override
    public String name()
    {
        return dataLayer.cqlTable().keyspace() + "." + dataLayer.cqlTable().table();
    }

    @Override
    public StructType schema()
    {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return new HashSet<>(ImmutableList.of(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ));
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        return new CassandraScanBuilder(dataLayer, schema, options);
    }
}
