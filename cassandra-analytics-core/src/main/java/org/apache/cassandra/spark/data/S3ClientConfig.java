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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.cassandra.spark.utils.MapUtils;

/**
 * Minimal S3 client configuration for connecting to S3.
 * This class contains only the essential settings needed to create an S3 client.
 * It is shared across batch and streaming configurations.
 */
public class S3ClientConfig implements Serializable
{
    private static final long serialVersionUID = 2L;

    // Option keys
    public static final String S3_REGION_KEY = "s3-region";
    public static final String S3_BUCKET_KEY = "s3-bucket";
    public static final String S3_ENDPOINT_OVERRIDE_KEY = "s3-endpoint-override";
    public static final String S3_ACCESS_KEY_ID_KEY = "s3-access-key-id";
    public static final String S3_SECRET_ACCESS_KEY_KEY = "s3-secret-access-key";
    public static final String S3_HTTP_MAX_CONCURRENCY_KEY = "s3-http-max-concurrency";

    @NotNull
    private final String s3Region;
    @NotNull
    private final String s3Bucket;
    @Nullable
    private final String s3EndpointOverride;
    @Nullable
    private final String s3AccessKeyId;
    @Nullable
    private final String s3SecretAccessKey;
    private final int s3HttpMaxConcurrency;

    private S3ClientConfig(@NotNull String s3Region,
                           @NotNull String s3Bucket,
                           @Nullable String s3EndpointOverride,
                           @Nullable String s3AccessKeyId,
                           @Nullable String s3SecretAccessKey,
                           int s3HttpMaxConcurrency)
    {
        if (s3HttpMaxConcurrency < 0)
        {
            throw new IllegalArgumentException("Invalid value for option '" + S3_HTTP_MAX_CONCURRENCY_KEY
                                               + "': " + s3HttpMaxConcurrency + " (must be >= 0)");
        }
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        this.s3EndpointOverride = s3EndpointOverride;
        this.s3AccessKeyId = s3AccessKeyId;
        this.s3SecretAccessKey = s3SecretAccessKey;
        this.s3HttpMaxConcurrency = s3HttpMaxConcurrency;
    }

    /**
     * Create an S3ClientConfig from a map of options.
     *
     * @param options Configuration options map (case-insensitive keys supported via MapUtils)
     * @return New S3ClientConfig instance
     * @throws IllegalArgumentException if required options (s3-region, s3-bucket) are missing
     */
    public static S3ClientConfig create(Map<String, String> options)
    {
        String s3Region = MapUtils.getOrThrow(options, S3_REGION_KEY, "region");
        String s3Bucket = MapUtils.getOrThrow(options, S3_BUCKET_KEY, "bucket");
        String s3EndpointOverride = options.get(S3_ENDPOINT_OVERRIDE_KEY);
        String s3AccessKeyId = options.get(S3_ACCESS_KEY_ID_KEY);
        String s3SecretAccessKey = options.get(S3_SECRET_ACCESS_KEY_KEY);
        int s3HttpMaxConcurrency = MapUtils.getInt(options, S3_HTTP_MAX_CONCURRENCY_KEY, 0);

        return new S3ClientConfig(s3Region, s3Bucket, s3EndpointOverride, s3AccessKeyId, s3SecretAccessKey,
                                  s3HttpMaxConcurrency);
    }

    /**
     * Create an S3ClientConfig with explicit values.
     *
     * @param s3Region           AWS region (required)
     * @param s3Bucket           S3 bucket name (required)
     * @param s3EndpointOverride Custom endpoint URL (optional, for LocalStack/MinIO)
     * @param s3AccessKeyId      AWS access key ID (optional, uses default credentials if null)
     * @param s3SecretAccessKey  AWS secret access key (optional, uses default credentials if null)
     * @return New S3ClientConfig instance
     */
    public static S3ClientConfig create(@NotNull String s3Region,
                                        @NotNull String s3Bucket,
                                        @Nullable String s3EndpointOverride,
                                        @Nullable String s3AccessKeyId,
                                        @Nullable String s3SecretAccessKey)
    {
        return create(s3Region, s3Bucket, s3EndpointOverride, s3AccessKeyId, s3SecretAccessKey, 0);
    }

    /**
     * Create an S3ClientConfig with explicit values.
     *
     * @param s3Region              AWS region (required)
     * @param s3Bucket              S3 bucket name (required)
     * @param s3EndpointOverride    Custom endpoint URL (optional, for LocalStack/MinIO)
     * @param s3AccessKeyId         AWS access key ID (optional, uses default credentials if null)
     * @param s3SecretAccessKey     AWS secret access key (optional, uses default credentials if null)
     * @param s3HttpMaxConcurrency  S3 async HTTP max concurrency; 0 means auto-size from Spark task slots
     * @return New S3ClientConfig instance
     */
    public static S3ClientConfig create(@NotNull String s3Region,
                                        @NotNull String s3Bucket,
                                        @Nullable String s3EndpointOverride,
                                        @Nullable String s3AccessKeyId,
                                        @Nullable String s3SecretAccessKey,
                                        int s3HttpMaxConcurrency)
    {
        return new S3ClientConfig(s3Region, s3Bucket, s3EndpointOverride, s3AccessKeyId, s3SecretAccessKey,
                                  s3HttpMaxConcurrency);
    }

    /**
     * Get the AWS region for S3 operations.
     */
    @NotNull
    public String s3Region()
    {
        return s3Region;
    }

    /**
     * Get the S3 bucket name.
     */
    @NotNull
    public String s3Bucket()
    {
        return s3Bucket;
    }

    /**
     * Get the custom S3 endpoint override URL.
     * Used for LocalStack, MinIO, or other S3-compatible services.
     *
     * @return Endpoint URL or null if using default AWS endpoint
     */
    @Nullable
    public String s3EndpointOverride()
    {
        return s3EndpointOverride;
    }

    /**
     * Get the AWS access key ID for static credentials.
     *
     * @return Access key ID or null if using default credentials provider
     */
    @Nullable
    public String s3AccessKeyId()
    {
        return s3AccessKeyId;
    }

    /**
     * Get the AWS secret access key for static credentials.
     *
     * @return Secret access key or null if using default credentials provider
     */
    @Nullable
    public String s3SecretAccessKey()
    {
        return s3SecretAccessKey;
    }

    /**
     * Get the async S3 HTTP client max concurrency override.
     * A value of 0 means the cache should auto-size from Spark executor task slots.
     */
    public int s3HttpMaxConcurrency()
    {
        return s3HttpMaxConcurrency;
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<S3ClientConfig>
    {
        @Override
        public void write(Kryo kryo, Output output, S3ClientConfig object)
        {
            output.writeString(object.s3Region);
            output.writeString(object.s3Bucket);
            output.writeString(object.s3EndpointOverride);
            output.writeString(object.s3AccessKeyId);
            output.writeString(object.s3SecretAccessKey);
            output.writeInt(object.s3HttpMaxConcurrency);
        }

        @Override
        public S3ClientConfig read(Kryo kryo, Input input, Class<S3ClientConfig> type)
        {
            return S3ClientConfig.create(input.readString(),
                                         input.readString(),
                                         input.readString(),
                                         input.readString(),
                                         input.readString(),
                                         input.readInt());
        }
    }
}
