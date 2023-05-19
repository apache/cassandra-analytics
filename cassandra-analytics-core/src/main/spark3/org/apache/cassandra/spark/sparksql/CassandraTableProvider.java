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

import java.util.Map;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public abstract class CassandraTableProvider implements TableProvider, DataSourceRegister
{
    private DataLayer dataLayer;

    public CassandraTableProvider()
    {
        CassandraBridgeFactory.validateBridges();
    }

    public abstract DataLayer getDataLayer(CaseInsensitiveStringMap options);

    DataLayer getDataLayerInternal(CaseInsensitiveStringMap options)
    {
        DataLayer dataLayer = this.dataLayer;
        if (dataLayer != null)
        {
            return dataLayer;
        }
        dataLayer = getDataLayer(options);
        this.dataLayer = dataLayer;
        return dataLayer;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        return getDataLayerInternal(options).structType();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties)
    {
        return new CassandraTable(getDataLayerInternal(new CaseInsensitiveStringMap(properties)), schema);
    }
}
