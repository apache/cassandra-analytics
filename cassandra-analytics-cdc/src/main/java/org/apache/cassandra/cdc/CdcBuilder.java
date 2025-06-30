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

package org.apache.cassandra.cdc;

import java.util.concurrent.ExecutorService;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.CommitLogProvider;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.api.StatePersister;
import org.apache.cassandra.cdc.api.TableIdLookup;
import org.apache.cassandra.cdc.api.TokenRangeSupplier;
import org.apache.cassandra.cdc.stats.CdcStats;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@SuppressWarnings("unused")  // external facing API
public class CdcBuilder
{
    @Nonnull
    protected final String jobId;
    protected final int partitionId;
    @Nonnull
    TokenRangeSupplier tokenRangeSupplier = () -> null;
    @Nonnull
    protected SchemaSupplier schemaSupplier;
    @Nonnull
    CassandraSource cassandraSource = CassandraSource.DEFAULT;
    @Nonnull
    StatePersister statePersister = StatePersister.STUB;
    @Nonnull
    protected CdcOptions cdcOptions = CdcOptions.DEFAULT;
    @Nonnull
    ICdcStats stats = ICdcStats.STUB;
    // asyncExecutor is @Nullable at compile time but required to be @Nonnull at runtime to build the Cdc object
    @SuppressWarnings("NonnullFieldNotInitialized")
    @Nonnull
    protected AsyncExecutor asyncExecutor = null;
    @Nullable
    protected CommitLogProvider commitLogProvider = null;
    @Nonnull
    protected EventConsumer eventConsumer;
    @Nonnull
    public TableIdLookup tableIdLookup = TableIdLookup.STUB;

    /**
     * @param jobId          unique jobId to identify this CDC job. It is used the namespace the CDC state so
     *                       that we can reset by changing the jobId or have multiple CDC streams running with unique jobIds.
     * @param partitionId    partitionId is any integer value to uniquely identify this shard of CDC.
     *                       This is only used for logging to assist with debugging, so each shard should have a unique partitionId.
     * @param eventConsumer  consumer that processes the CDC updates as they are received.
     * @param schemaSupplier provides the schema for the CDC enabled tables.
     */
    public CdcBuilder(@Nonnull String jobId,
                      int partitionId,
                      @Nonnull EventConsumer eventConsumer,
                      @Nonnull SchemaSupplier schemaSupplier)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.eventConsumer = eventConsumer;
        this.schemaSupplier = schemaSupplier;
    }

    /**
     * @return a Cassandra Token Range that this consumer should read from.
     * This supplier is called at the start of each micro-batch to permit topology changes between micro-batches.
     * Returning a null token range means it will attempt to read all available commit logs.
     */
    public CdcBuilder withTokenRangeSupplier(@Nonnull TokenRangeSupplier tokenRangeSupplier)
    {
        this.tokenRangeSupplier = tokenRangeSupplier;
        return this;
    }

    public CdcBuilder withCassandraSource(@Nonnull CassandraSource cassandraSource)
    {
        this.cassandraSource = cassandraSource;
        return this;
    }

    public CdcBuilder withStatePersister(@Nonnull StatePersister statePersister)
    {
        this.statePersister = statePersister;
        return this;
    }

    public CdcBuilder withCdcOptions(@Nonnull CdcOptions cdcOptions)
    {
        this.cdcOptions = cdcOptions;
        return this;
    }

    public CdcBuilder withExecutor(@Nonnull ExecutorService executor)
    {
        return withExecutor(AsyncExecutor.wrap(executor));
    }


    public CdcBuilder withExecutor(@Nonnull AsyncExecutor asyncExecutor)
    {
        this.asyncExecutor = asyncExecutor;
        return this;
    }

    public CdcBuilder withCommitLogProvider(@Nonnull CommitLogProvider commitLogProvider)
    {
        this.commitLogProvider = commitLogProvider;
        return this;
    }

    public CdcBuilder withStats(@Nonnull CdcStats stats)
    {
        this.stats = stats;
        return this;
    }

    public CdcBuilder withEventConsumer(@Nonnull EventConsumer eventConsumer)
    {
        this.eventConsumer = eventConsumer;
        return this;
    }

    public CdcBuilder withTableIdLookup(@Nonnull TableIdLookup tableIdLookup)
    {
        this.tableIdLookup = tableIdLookup;
        return this;
    }

    public CdcBuilder withSchemaSupplier(@Nonnull SchemaSupplier schemaSupplier)
    {
        this.schemaSupplier = schemaSupplier;
        return this;
    }

    public Cdc build()
    {
        Preconditions.checkNotNull(commitLogProvider, "A CommitLogProvider must be supplied");
        Preconditions.checkNotNull(asyncExecutor, "An AsyncExecutor must be supplied");
        Preconditions.checkNotNull(eventConsumer, "An event consumer supplier must be supplied");
        Preconditions.checkNotNull(schemaSupplier, "An schema supplier must be supplied");
        return new Cdc(this);
    }
}
