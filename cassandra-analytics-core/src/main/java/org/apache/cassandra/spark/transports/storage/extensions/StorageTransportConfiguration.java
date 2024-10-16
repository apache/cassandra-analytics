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

package org.apache.cassandra.spark.transports.storage.extensions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.apache.cassandra.spark.transports.storage.StorageAccessConfiguration;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.jetbrains.annotations.Nullable;

/**
 * Holds information about the cloud storage configuration
 */
public class StorageTransportConfiguration
{
    private final String prefix;
    private final Map<String, String> tags;
    // many read access configurations
    private final MultiClusterContainer<StorageAccessConfiguration> readAccessConfigurations;
    // one write access configuration
    private StorageAccessConfiguration writeAccessConfiguration;

    // Same constructor for backward compatibility
    public StorageTransportConfiguration(String writeBucket, String writeRegion,
                                         String readBucket, String readRegion,
                                         String prefix,
                                         StorageCredentialPair storageCredentialPair,
                                         Map<String, String> tags)
    {
        this(prefix, tags,
             new StorageAccessConfiguration(writeRegion, writeBucket, storageCredentialPair.writeCredentials),
             new MultiClusterContainer<>());
        StorageAccessConfiguration readAccess = new StorageAccessConfiguration(readRegion, readBucket, storageCredentialPair.readCredentials);
        readAccessConfigurations.setValue(null, readAccess);
    }

    // Constructor for coordinated-write use case
    public StorageTransportConfiguration(String prefix,
                                         Map<String, String> tags,
                                         StorageAccessConfiguration writeAccessConfiguration,
                                         Map<String, StorageAccessConfiguration> readAccessConfigByCluster)
    {
        this(prefix, tags, writeAccessConfiguration, new MultiClusterContainer<>());
        this.readAccessConfigurations.addAll(readAccessConfigByCluster);
    }

    private StorageTransportConfiguration(String prefix,
                                          Map<String, String> tags,
                                          StorageAccessConfiguration writeAccessConfiguration,
                                          MultiClusterContainer<StorageAccessConfiguration> readAccessConfigurations)
    {
        this.prefix = prefix;
        this.writeAccessConfiguration = writeAccessConfiguration;
        this.readAccessConfigurations = readAccessConfigurations;
        this.tags = Collections.unmodifiableMap(tags);
    }

    public StorageAccessConfiguration writeAccessConfiguration()
    {
        return writeAccessConfiguration;
    }

    /**
     * @param clusterId cluster id. Cluster id must present for coordinated write; otherwise, it is null
     * @return read access configuration
     */
    public StorageAccessConfiguration readAccessConfiguration(@Nullable String clusterId)
    {
        return readAccessConfigurations.getValueOrNull(clusterId);
    }

    /**
     * @param clusterId cluster id. Cluster id must present for coordinated write; otherwise, it is null
     * @return a map of access tokens used to authenticate to the storage transport
     */
    public StorageCredentialPair getStorageCredentialPair(@Nullable String clusterId)
    {
        StorageAccessConfiguration readAccess = readAccessConfigurations.getValueOrNull(clusterId);
        return new StorageCredentialPair(writeAccessConfiguration.region(),
                                         writeAccessConfiguration.storageCredentials(),
                                         readAccess.region(),
                                         readAccess.storageCredentials());
    }

    /**
     * @param clusterId cluster id. Cluster id must present for coordinated write; otherwise, it is null
     * @param newCredentials new set of access tokens
     */
    public void setStorageCredentialPair(@Nullable String clusterId, StorageCredentialPair newCredentials)
    {
        writeAccessConfiguration = writeAccessConfiguration.copyWithNewCredentials(newCredentials.writeCredentials);
        readAccessConfigurations.updateValue(clusterId, readAccess -> readAccess.copyWithNewCredentials(newCredentials.readCredentials));
    }

    public String getPrefix()
    {
        return prefix;
    }

    public Map<String, String> getTags()
    {
        return tags;
    }

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
        StorageTransportConfiguration that = (StorageTransportConfiguration) o;
        return Objects.equals(prefix, that.prefix)
               && Objects.equals(tags, that.tags)
               && Objects.equals(writeAccessConfiguration, that.writeAccessConfiguration)
               && Objects.equals(readAccessConfigurations, that.readAccessConfigurations);
    }

    public int hashCode()
    {
        return Objects.hash(prefix, tags, writeAccessConfiguration, readAccessConfigurations);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<StorageTransportConfiguration>
    {
        public void write(Kryo kryo, Output out, StorageTransportConfiguration obj)
        {
            out.writeString(obj.prefix);
            kryo.writeObject(out, obj.tags);
            kryo.writeObject(out, obj.writeAccessConfiguration);
            kryo.writeObject(out, obj.readAccessConfigurations);
        }

        @SuppressWarnings("unchecked")
        public StorageTransportConfiguration read(Kryo kryo, Input in, Class<StorageTransportConfiguration> type)
        {
            return new StorageTransportConfiguration(in.readString(),
                                                     kryo.readObject(in, HashMap.class),
                                                     kryo.readObject(in, StorageAccessConfiguration.class),
                                                     kryo.readObject(in, MultiClusterContainer.class));
        }
    }
}
