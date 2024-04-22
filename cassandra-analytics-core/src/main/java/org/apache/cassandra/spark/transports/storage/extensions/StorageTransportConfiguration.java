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

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;

/**
 * Holds information about the Blob configuration
 */
public class StorageTransportConfiguration implements Serializable
{
    private static final long serialVersionUID = -8164878804296039585L;
    private final String writeBucket;
    private final String writeRegion;
    private final String readBucket;
    private final String readRegion;
    private final String prefix;
    private final Map<String, String> tags;
    private StorageCredentialPair storageCredentialPair;

    public StorageTransportConfiguration(String writeBucket, String writeRegion,
                                         String readBucket, String readRegion,
                                         String prefix,
                                         StorageCredentialPair storageCredentialPair,
                                         Map<String, String> tags)
    {
        this.writeBucket = writeBucket;
        this.writeRegion = writeRegion;
        this.readBucket = readBucket;
        this.readRegion = readRegion;
        this.prefix = prefix;
        this.storageCredentialPair = storageCredentialPair;
        this.tags = Collections.unmodifiableMap(tags);
    }

    /**
     * @return the base {@link URI} to use for accessing the storage transport
     */
    public String getWriteBucket()
    {
        return writeBucket;
    }

    /**
     * @return a map of access tokens used to authenticate to the storage transport
     */
    public StorageCredentialPair getStorageCredentialPair()
    {
        return storageCredentialPair;
    }

    public void setBlobCredentialPair(StorageCredentialPair newCredentials)
    {
        this.storageCredentialPair = newCredentials;
    }

    public String getWriteRegion()
    {
        return writeRegion;
    }

    public String getReadBucket()
    {
        return readBucket;
    }

    public String getReadRegion()
    {
        return readRegion;
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
        return Objects.equals(writeBucket, that.writeBucket)
               && Objects.equals(writeRegion, that.writeRegion)
               && Objects.equals(readBucket, that.readBucket)
               && Objects.equals(readRegion, that.readRegion)
               && Objects.equals(prefix, that.prefix)
               && Objects.equals(storageCredentialPair, that.storageCredentialPair)
               && Objects.equals(tags, that.tags);
    }

    public int hashCode()
    {
        return Objects.hash(writeBucket, writeRegion, readBucket, readRegion, prefix, storageCredentialPair, tags);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<StorageTransportConfiguration>
    {
        public void write(Kryo kryo, Output out, StorageTransportConfiguration obj)
        {
            out.writeString(obj.writeBucket);
            out.writeString(obj.writeRegion);
            out.writeString(obj.readBucket);
            out.writeString(obj.readRegion);
            out.writeString(obj.prefix);
            kryo.writeObject(out, obj.storageCredentialPair);
            kryo.writeObject(out, obj.tags);
        }

        @SuppressWarnings("unchecked")
        public StorageTransportConfiguration read(Kryo kryo, Input in, Class<StorageTransportConfiguration> type)
        {
            return new StorageTransportConfiguration(in.readString(),
                                                     in.readString(),
                                                     in.readString(),
                                                     in.readString(),
                                                     in.readString(),
                                                     kryo.readObject(in, StorageCredentialPair.class),
                                                     kryo.readObject(in, HashMap.class));
        }
    }
}
