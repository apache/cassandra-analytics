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

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;

/**
 * A class representing the pair of credentials needed to complete an analytics operation using the Storage transport.
 * It is possible that both credentials (read and write) are the same, but also that they could represent
 * the credentials needed for two different buckets when using cross-region synchronization to transfer data
 * between regions.
 */
public class StorageCredentialPair
{
    private final String writeRegion;
    public final StorageCredentials writeCredentials;
    private final String readRegion;
    public final StorageCredentials readCredentials;

    /**
     * Create a new instance of a StorageCredentialPair
     *
     * @param writeRegion the name of the region where write/upload happens
     * @param writeCredentials the credentials used for writing to the storage endpoint
     * @param readRegion the name of the region where read/download happens
     * @param readCredentials  the credentials used to read from the storage endpoint
     */
    public StorageCredentialPair(String writeRegion,
                                 StorageCredentials writeCredentials,
                                 String readRegion,
                                 StorageCredentials readCredentials)
    {
        this.writeRegion = writeRegion;
        this.writeCredentials = writeCredentials;
        this.readRegion = readRegion;
        this.readCredentials = readCredentials;
    }

    public RestoreJobSecrets toRestoreJobSecrets()
    {
        return new RestoreJobSecrets(readCredentials.toSidecarCredentials(readRegion),
                                     writeCredentials.toSidecarCredentials(writeRegion));
    }

    @Override
    public String toString()
    {
        return "StorageCredentialPair{"
               + "writeRegion=" + writeRegion
               + ", writeCredentials=" + writeCredentials
               + ", readRegion=" + readRegion
               + ", readCredentials=" + readCredentials
               + '}';
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
        return Objects.equals(writeRegion, that.writeRegion)
               && Objects.equals(writeCredentials, that.writeCredentials)
               && Objects.equals(readRegion, that.readRegion)
               && Objects.equals(readCredentials, that.readCredentials);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(writeRegion, writeCredentials, readRegion, readCredentials);
    }
}
