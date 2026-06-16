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
    private final StorageAuth storageAuth;

    public StorageAccessConfiguration(@NotNull String region, @NotNull String bucket,
                                      @NotNull StorageAuth storageAuth)
    {
        this.region = region;
        this.bucket = bucket;
        this.storageAuth = storageAuth;
    }

    /**
     * Create a new instance of StorageAccessConfiguration with the auth updated.
     *
     * @param newAuth the new auth mechanism
     * @return new instance
     */
    public StorageAccessConfiguration copyWithNewAuth(@NotNull StorageAuth newAuth)
    {
        return new StorageAccessConfiguration(this.region, this.bucket, newAuth);
    }

    public String region()
    {
        return region;
    }

    public String bucket()
    {
        return bucket;
    }

    public StorageAuth storageAuth()
    {
        return storageAuth;
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
               && Objects.equals(storageAuth, that.storageAuth);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(region, bucket, storageAuth);
    }

    @Override
    public String toString()
    {
        return "StorageAccessConfiguration{"
               + "region='" + region + '\''
               + ", bucket='" + bucket + '\''
               + ", auth='" + storageAuth + '\''
               + '}';
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<StorageAccessConfiguration>
    {
        @Override
        public void write(Kryo kryo, Output out, StorageAccessConfiguration object)
        {
            out.writeString(object.region);
            out.writeString(object.bucket);
            String authType = object.storageAuth instanceof IamStorageAuth ? "IAM" : "STATIC";
            out.writeString(authType);
            if (!"IAM".equals(authType))
            {
                kryo.writeObject(out, (StorageCredentials) object.storageAuth);
            }
        }

        @Override
        public StorageAccessConfiguration read(Kryo kryo, Input in, Class<StorageAccessConfiguration> type)
        {
            String region = in.readString();
            String bucket = in.readString();
            String authType = in.readString();
            StorageAuth auth = "IAM".equals(authType) ? IamStorageAuth.INSTANCE : kryo.readObject(in, StorageCredentials.class);
            return new StorageAccessConfiguration(region, bucket, auth);
        }
    }
}
