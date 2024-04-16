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

import java.io.Serializable;
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;

/**
 * A class representing the pair of credentials needed to complete an analytics operation using the Storage transport.
 * It is possible that both credentials (read and write) will be the same, but also that they could represent
 * the credentials needed for two different buckets when using cross-region synchronization to transfer data
 * between regions.
 */
public class StorageCredentialPair implements Serializable
{
    private static final long serialVersionUID = 6084829690503608102L;
    StorageCredentials writeCredentials;
    StorageCredentials readCredentials;

    /**
     * Create a new instance of a StorageCredentialPair
     *
     * @param writeCredentials the credentials used for writing to the storage endpoint.
     * @param readCredentials  the credentials used to read from the storage endpoint.
     */
    public StorageCredentialPair(StorageCredentials writeCredentials, StorageCredentials readCredentials)
    {
        this.writeCredentials = writeCredentials;
        this.readCredentials = readCredentials;
    }

    public StorageCredentials getWriteCredentials()
    {
        return writeCredentials;
    }

    public StorageCredentials getReadCredentials()
    {
        return readCredentials;
    }

    @Override
    public String toString()
    {
        return "StorageCredentialPair{"
               + "writeCredentials=" + writeCredentials
               + ", readCredentials=" + readCredentials
               + '}';
    }

    public RestoreJobSecrets toRestoreJobSecrets(String readRegion, String writeRegion)
    {
        return new RestoreJobSecrets(readCredentials.toSidecarCredentials(readRegion),
                                     writeCredentials.toSidecarCredentials(writeRegion));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof StorageCredentialPair))
        {
            return false;
        }
        StorageCredentialPair that = (StorageCredentialPair) o;
        return Objects.equals(writeCredentials, that.writeCredentials) && Objects.equals(readCredentials, that.readCredentials);
    }

    public int hashCode()
    {
        return Objects.hash(writeCredentials, readCredentials);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<StorageCredentialPair>
    {

        public void write(Kryo kryo, Output out, StorageCredentialPair object)
        {
            kryo.writeObject(out, object.writeCredentials);
            kryo.writeObject(out, object.readCredentials);
        }

        public StorageCredentialPair read(Kryo kryo, Input in, Class<StorageCredentialPair> type)
        {
            return new StorageCredentialPair(
            kryo.readObject(in, StorageCredentials.class),
            kryo.readObject(in, StorageCredentials.class)
            );
        }
    }
}
