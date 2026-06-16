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
 * A class representing the pair of auth configurations needed to complete an analytics operation using the
 * Storage transport. It is possible that both read and write auth are the same, but they could also represent
 * different buckets when using cross-region synchronization to transfer data between regions.
 *
 * <p>The auth field is a {@link StorageAuth}, either:
 * <ul>
 *   <li>{@link StorageCredentials} — explicit STS credentials for static auth</li>
 *   <li>{@link IamStorageAuth} — no static credentials; the sidecar and Spark executors use the AWS SDK
 *       default provider chain (instance profile / IRSA / ECS task role)</li>
 * </ul>
 *
 * <p>For IAM mode use {@link #iamPair(String, String)}. The library wires the correct sidecar payload
 * automatically when {@code STORAGE_CREDENTIAL_TYPE=IAM} is set.
 */
public class StorageCredentialPair
{
    private final String writeRegion;
    private final StorageAuth writeAuth;
    private final String readRegion;
    private final StorageAuth readAuth;

    /**
     * Creates a {@link StorageCredentialPair} for IAM instance profile mode.
     * The credentials fields are null; only the regions are required so the sidecar can route requests.
     *
     * @param readRegion  the AWS region for the read (download) bucket
     * @param writeRegion the AWS region for the write (upload) bucket
     * @return a region-only pair suitable for use with {@code STORAGE_CREDENTIAL_TYPE=IAM}
     */
    public static StorageCredentialPair iamPair(String readRegion, String writeRegion)
    {
        return new StorageCredentialPair(writeRegion, IamStorageAuth.INSTANCE, readRegion, IamStorageAuth.INSTANCE);
    }

    /**
     * Create a new instance of a StorageCredentialPair
     *
     * @param writeRegion the name of the region where write/upload happens
     * @param writeAuth   the auth used for writing to the storage endpoint
     * @param readRegion  the name of the region where read/download happens
     * @param readAuth    the auth used for reading from the storage endpoint
     */
    public StorageCredentialPair(String writeRegion,
                                 StorageAuth writeAuth,
                                 String readRegion,
                                 StorageAuth readAuth)
    {
        this.writeRegion = writeRegion;
        this.writeAuth = writeAuth;
        this.readRegion = readRegion;
        this.readAuth = readAuth;
    }

    public String writeRegion()
    {
        return writeRegion;
    }

    public StorageAuth writeAuth()
    {
        return writeAuth;
    }

    public String readRegion()
    {
        return readRegion;
    }

    public StorageAuth readAuth()
    {
        return readAuth;
    }

    /**
     * Converts this pair to {@link RestoreJobSecrets} for static credential mode.
     * Throws {@link IllegalStateException} if either auth is not a {@link StorageCredentials} (i.e. this is an IAM pair);
     * use {@link RestoreJobSecrets#iamMode(String, String)} for IAM mode.
     */
    public RestoreJobSecrets toRestoreJobSecrets()
    {
        if (!(writeAuth instanceof StorageCredentials) || !(readAuth instanceof StorageCredentials))
        {
            throw new IllegalStateException("Cannot call toRestoreJobSecrets() on an IAM credential pair. " +
                                            "Use RestoreJobSecrets.iamMode() instead.");
        }
        StorageCredentials write = (StorageCredentials) writeAuth;
        StorageCredentials read = (StorageCredentials) readAuth;
        return new RestoreJobSecrets(read.toSidecarCredentials(readRegion),
                                     write.toSidecarCredentials(writeRegion));
    }

    @Override
    public String toString()
    {
        return "StorageCredentialPair{"
               + "writeRegion=" + writeRegion
               + ", writeAuth=" + writeAuth
               + ", readRegion=" + readRegion
               + ", readAuth=" + readAuth
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
               && Objects.equals(writeAuth, that.writeAuth)
               && Objects.equals(readRegion, that.readRegion)
               && Objects.equals(readAuth, that.readAuth);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(writeRegion, writeAuth, readRegion, readAuth);
    }
}
