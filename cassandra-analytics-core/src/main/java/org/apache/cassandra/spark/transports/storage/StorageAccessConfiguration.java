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

package org.apache.cassandra.spark.transports.storage;

import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jetbrains.annotations.NotNull;

/**
 * Holds relevant information to access the bucket in the region
 */
public class StorageAccessConfiguration
{
    private final String region;
    private final String bucket;
    private final StorageCredentials storageCredentials;

    public StorageAccessConfiguration(@NotNull String region, @NotNull String bucket, @NotNull StorageCredentials storageCredentials)
    {
        this.region = region;
        this.bucket = bucket;
        this.storageCredentials = storageCredentials;
    }

    /**
     * Create a new instance of StorageAccessConfiguration with the credentials updated
     * @param newCredentials credentials to update
     * @return new instance
     */
    public StorageAccessConfiguration copyWithNewCredentials(@NotNull StorageCredentials newCredentials)
    {
        return new StorageAccessConfiguration(this.region, this.bucket, newCredentials);
    }

    public String region()
    {
        return region;
    }

    public String bucket()
    {
        return bucket;
    }

    public StorageCredentials storageCredentials()
    {
        return storageCredentials;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof StorageAccessConfiguration))
        {
            return false;
        }

        StorageAccessConfiguration that = (StorageAccessConfiguration) obj;
        return Objects.equals(region, that.region)
               && Objects.equals(bucket, that.bucket)
               && Objects.equals(storageCredentials, that.storageCredentials);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(region, bucket, storageCredentials);
    }

    @Override
    public String toString()
    {
        return "StorageAccessConfiguration{"
               + "region='" + region + '\''
               + ", bucket='" + bucket + '\''
               + ", credentials='" + storageCredentials + '\''
               + '}';
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<StorageAccessConfiguration>
    {
        @Override
        public void write(Kryo kryo, Output out, StorageAccessConfiguration object)
        {
            out.writeString(object.region);
            out.writeString(object.bucket);
            kryo.writeObject(out, object.storageCredentials);
        }

        @Override
        public StorageAccessConfiguration read(Kryo kryo, Input in, Class<StorageAccessConfiguration> type)
        {
            return new StorageAccessConfiguration(
            in.readString(), // region
            in.readString(), // bucket
            kryo.readObject(in, StorageCredentials.class));
        }
    }
}
