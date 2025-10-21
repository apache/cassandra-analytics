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

import java.io.Serializable;

import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable configuration data class for BulkWriter jobs that is safe to broadcast to Spark executors.
 * This class contains only pure configuration data and no stateful objects or lifecycle methods.
 * <p>
 * BulkWriterContext instances should be constructed from this config on both driver and executors
 * using {@link BulkWriterContext#from(BulkWriterConfig, boolean)}.
 */
public final class BulkWriterConfig implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final BulkSparkConf conf;
    private final StructType structType;
    private final int sparkDefaultParallelism;

    /**
     * Creates a new immutable BulkWriterConfig
     *
     * @param conf                    Bulk writer Spark configuration
     * @param structType              DataFrame schema structure
     * @param sparkDefaultParallelism Spark default parallelism setting
     */
    public BulkWriterConfig(@NotNull BulkSparkConf conf,
                            @NotNull StructType structType,
                            int sparkDefaultParallelism)
    {
        this.conf = conf;
        this.structType = structType;
        this.sparkDefaultParallelism = sparkDefaultParallelism;
    }

    public BulkSparkConf getConf()
    {
        return conf;
    }

    public StructType getStructType()
    {
        return structType;
    }

    public int getSparkDefaultParallelism()
    {
        return sparkDefaultParallelism;
    }
}
