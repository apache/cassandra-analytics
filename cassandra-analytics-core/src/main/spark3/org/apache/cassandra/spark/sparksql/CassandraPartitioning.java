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

import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;

/**
 * Reports the number of Spark partitions to the planner via {@link UnknownPartitioning}.
 *
 * <p>Spark 3.3+ replaced the older {@code Partitioning#satisfy(Distribution)} contract with
 * concrete partitioning types. The Cassandra reader can report its partition count, but not a
 * key grouping Spark can safely use for storage-partitioned joins.
 *
 * <p>Each {@link CassandraInputPartition} is a token range. Although each row's token is derived
 * from the partition key, rows in the same token range usually have many distinct partition keys
 * and many distinct tokens. That does not satisfy {@code KeyGroupedPartitioning}'s contract that
 * every row in one Spark partition evaluates to the same partition value.
 */
class CassandraPartitioning extends UnknownPartitioning
{
    CassandraPartitioning(int numPartitions)
    {
        super(numPartitions);
    }
}
