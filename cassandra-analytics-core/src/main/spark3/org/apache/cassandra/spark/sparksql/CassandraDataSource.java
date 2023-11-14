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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.CassandraDataSourceHelper;
import org.apache.cassandra.spark.data.ClientConfig;
import org.apache.cassandra.spark.data.DataLayer;

public class CassandraDataSource extends CassandraTableProvider
{
    @Override
    public String shortName()
    {
        return "cassandraBulkRead";
    }

    @Override
    public DataLayer getDataLayer(org.apache.spark.sql.util.CaseInsensitiveStringMap options)
    {
        return CassandraDataSourceHelper.getDataLayer(options, this::initializeDataLayer);
    }

    @VisibleForTesting
    protected void initializeDataLayer(CassandraDataLayer dataLayer, ClientConfig config)
    {
        dataLayer.initialize(config);
    }
}
