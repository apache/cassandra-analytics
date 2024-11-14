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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

// CHECKSTYLE IGNORE: This class cannot be declared as final, because consumers should be able to extend it
public class CassandraBulkWriterContext extends AbstractBulkWriterContext
{
    private static final long serialVersionUID = 8241993502687688783L;

    protected CassandraBulkWriterContext(@NotNull BulkSparkConf conf,
                                         @NotNull StructType structType,
                                         int sparkDefaultParallelism)
    {
        super(conf, structType, sparkDefaultParallelism);
    }

    @Override
    protected ClusterInfo buildClusterInfo()
    {
        return new CassandraClusterInfo(bulkSparkConf());
    }

    @Override
    protected void validateKeyspaceReplication()
    {
        BulkSparkConf conf = bulkSparkConf();
        // no validation for non-local CL
        if (!conf.consistencyLevel.isLocal())
        {
            return;
        }
        // localDc is not empty and replication option contains localDc
        boolean isReplicatedToLocalDc = !StringUtils.isEmpty(conf.localDC)
                                        && cluster().replicationFactor().getOptions().containsKey(conf.localDC);
        Preconditions.checkState(isReplicatedToLocalDc, "Keyspace %s is not replicated on datacenter %s", conf.keyspace, conf.localDC);
    }
}
