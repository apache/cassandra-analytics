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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.Serializable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Note: Must use the sidecar shaded jackson to ser/deser sidecar objects
import o.a.c.sidecar.client.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import o.a.c.sidecar.client.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.jetbrains.annotations.NotNull;

/**
 * A serializable wrapper of {@link CreateSliceRequestPayload} and also implements hashcode and equals
 */
public class CreatedRestoreSlice implements Serializable
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(CreatedRestoreSlice.class);
    private static final long serialVersionUID = 1738928448022537598L;

    private transient CreateSliceRequestPayload sliceRequestPayload;
    private transient Set<CassandraInstance> succeededInstances;
    private transient boolean isSatisfied = false;
    public final String sliceRequestPayloadJson; // equals and hashcode use and only implement with this field

    public CreatedRestoreSlice(@NotNull CreateSliceRequestPayload sliceRequestPayload)
    {
        this.sliceRequestPayload = sliceRequestPayload;
        this.sliceRequestPayloadJson = toJson(sliceRequestPayload);
    }

    public CreateSliceRequestPayload sliceRequestPayload()
    {
        if (sliceRequestPayloadJson == null)
        {
            throw new IllegalStateException("sliceRequestPayloadJson cannot be null");
        }

        if (sliceRequestPayload != null)
        {
            return sliceRequestPayload;
        }

        // The following code could run multiple times if in a multi-threads environment.
        // It is relatively cheap to deserialize, hence that multiple runs is acceptable.
        try
        {
            sliceRequestPayload = MAPPER.readValue(sliceRequestPayloadJson, CreateSliceRequestPayload.class);
            return sliceRequestPayload;
        }
        catch (Exception exception)
        {
            LOGGER.error("Unable to deserialize CreateSliceRequestPayload from JSON. requestPayloadJson={}",
                         sliceRequestPayloadJson, exception);
            throw new RuntimeException("Unable to deserialize CreateSliceRequestPayload from JSON", exception);
        }
    }

    public void addSucceededInstance(CassandraInstance instance)
    {
        succeededInstances().add(instance);
    }

    /**
     * Check whether the slice satisfies the consistency level
     *
     * @param consistencyLevel  consistency level to check
     * @param replicationFactor replication factor to check
     * @param localDC           local DC name if any
     * @return check result, either not satisfied, satisfied, or already satisfied
     */
    public synchronized ConsistencyLevelCheckResult checkForConsistencyLevel(ConsistencyLevel consistencyLevel,
                                                                             ReplicationFactor replicationFactor,
                                                                             String localDC)
    {
        if (isSatisfied)
        {
            return ConsistencyLevelCheckResult.ALREADY_SATISFIED;
        }

        if (!succeededInstances().isEmpty()
            && consistencyLevel.canBeSatisfied(succeededInstances(), Collections.emptyList(), replicationFactor, localDC))
        {
            isSatisfied = true;
            return ConsistencyLevelCheckResult.SATISFIED;
        }

        return ConsistencyLevelCheckResult.NOT_SATISFIED;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        CreatedRestoreSlice that = (CreatedRestoreSlice) o;
        return Objects.equals(sliceRequestPayloadJson, that.sliceRequestPayloadJson);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sliceRequestPayloadJson);
    }

    @Override
    public String toString()
    {
        return sliceRequestPayload.toString();
    }

    Set<CassandraInstance> succeededInstances()
    {
        Set<CassandraInstance> currentInstances = succeededInstances;
        if (currentInstances != null)
        {
            return currentInstances;
        }

        synchronized (this)
        {
            if (succeededInstances == null)
            {
                succeededInstances = ConcurrentHashMap.newKeySet();
            }
        }
        return succeededInstances;
    }

    private static String toJson(@NotNull CreateSliceRequestPayload sliceRequestPayload)
    {
        try
        {
            return MAPPER.writeValueAsString(sliceRequestPayload);
        }
        catch (JsonProcessingException jsonProcessingException)
        {
            LOGGER.error("Unable to serialize CreateSliceRequestPayload to JSON. requestPayload={}",
                         sliceRequestPayload, jsonProcessingException);
            throw new RuntimeException("Unable to serialize CreateSliceRequestPayload to JSON", jsonProcessingException);
        }
    }

    public enum ConsistencyLevelCheckResult
    {
        NOT_SATISFIED,
        SATISFIED,
        ALREADY_SATISFIED;
    }
}
