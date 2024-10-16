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

/**
 * StorageCredentials are used to represent the security information required to read from or write to a storage endpoint.
 * Storage credentials can be either an access key ID and secret key, or also include a sessionToken when using
 * temporary a temporary IAM credential.
 */
public class StorageCredentials
{
    private final String accessKeyId;
    private final String secretKey;
    private final String sessionToken;

    public static StorageCredentials fromSidecarCredentials(o.a.c.sidecar.client.shaded.common.data.StorageCredentials credentials)
    {
        return new StorageCredentials(credentials.accessKeyId(),
                                      credentials.secretAccessKey(),
                                      credentials.sessionToken());
    }

    /**
     * Creates a Storage Credential instance with only an Access Key and Secret Key.
     *
     * @param accessKeyId the accessKeyId to use to access the S3 bucket
     * @param secretKey   the secretKey to use to access the S3 Bucket
     */
    public StorageCredentials(String accessKeyId, String secretKey)
    {
        this(accessKeyId, secretKey, null);
    }

    /**
     * Creates a Storage Credential instance with only an Access Key, Secret Key, and Session Token.
     * Used when a temporary IAM credential is to be provided to S3 for authentication/authorization.
     * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/AuthUsingTempSessionToken.html">The Amazon
     * Documentation on Temporary IAM Credentials</a>
     * for more details.
     *
     * @param accessKeyId  the accessKeyId to use to access the S3 bucket
     * @param secretKey    the secretKey to use to access the S3 Bucket
     * @param sessionToken the session token to use to access the S3 Bucket
     */
    public StorageCredentials(String accessKeyId, String secretKey, String sessionToken)
    {
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
    }

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public String getSessionToken()
    {
        return sessionToken;
    }

    public o.a.c.sidecar.client.shaded.common.data.StorageCredentials toSidecarCredentials(String region)
    {
        return o.a.c.sidecar.client.shaded.common.data.StorageCredentials
               .builder()
               .accessKeyId(accessKeyId)
               .secretAccessKey(secretKey)
               .sessionToken(sessionToken)
               .region(region)
               .build();
    }

    @Override
    public String toString()
    {
        return "StorageCredentials{"
               + "accessKeyId='" + accessKeyId + '\''
               + ", secretKey='" + redact(secretKey) + '\''
               + ", sessionToken='" + redact(sessionToken) + '\''
               + '}';
    }

    private String redact(String value)
    {
        if (value == null || value.isEmpty())
        {
            return "NOT_PROVIDED";
        }
        return "*****";
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
        StorageCredentials that = (StorageCredentials) o;
        return Objects.equals(accessKeyId, that.accessKeyId)
               && Objects.equals(secretKey, that.secretKey)
               && Objects.equals(sessionToken, that.sessionToken);
    }

    public int hashCode()
    {
        return Objects.hash(accessKeyId, secretKey, sessionToken);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<StorageCredentials>
    {
        public void write(Kryo kryo, Output out, StorageCredentials obj)
        {
            out.writeString(obj.accessKeyId);
            out.writeString(obj.secretKey);
            out.writeString(obj.sessionToken);
        }

        public StorageCredentials read(Kryo kryo, Input input, Class<StorageCredentials> type)
        {
            return new StorageCredentials(
            input.readString(),
            input.readString(),
            input.readString()
            );
        }
    }
}
