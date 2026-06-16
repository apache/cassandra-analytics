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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraScanBuilderTest
{
    @Test
    void outputPartitioningReportsUnknownPartitioningWithPartitionCount()
    {
        DataLayer dataLayer = mock(DataLayer.class);
        when(dataLayer.partitionCount()).thenReturn(7);
        CassandraScanBuilder builder =
            new CassandraScanBuilder(dataLayer, new StructType(), CaseInsensitiveStringMap.empty());

        Partitioning partitioning = builder.outputPartitioning();

        assertThat(partitioning).isInstanceOf(UnknownPartitioning.class);
        assertThat(partitioning.numPartitions()).isEqualTo(7);
    }
}
