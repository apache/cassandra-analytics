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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.sql.connector.read.partitioning.ClusteredDistribution;
import org.apache.spark.sql.connector.read.partitioning.Distribution;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;

class CassandraPartitioning implements Partitioning
{
    final DataLayer dataLayer;

    CassandraPartitioning(DataLayer dataLayer)
    {
        this.dataLayer = dataLayer;
    }

    @Override
    public int numPartitions()
    {
        return dataLayer.partitionCount();
    }

    @Override
    public boolean satisfy(Distribution distribution)
    {
        if (distribution instanceof ClusteredDistribution)
        {
            String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
            List<String> partitionKeys = dataLayer.cqlTable().partitionKeys().stream()
                                                                             .map(CqlField::name)
                                                                             .collect(Collectors.toList());
            return Arrays.asList(clusteredCols).containsAll(partitionKeys);
        }
        return false;
    }
}
