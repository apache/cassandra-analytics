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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link S3ClientConfig}.
 */
class S3ClientConfigTest
{
    private static final Map<String, String> REQUIRED_OPTIONS = ImmutableMap.<String, String>builder()
        .put("s3-region", "us-west-2")
        .put("s3-bucket", "test-bucket")
        .build();

    @Test
    void testCreateWithRequiredOptions()
    {
        S3ClientConfig config = S3ClientConfig.create(new CaseInsensitiveStringMap(REQUIRED_OPTIONS));

        assertThat(config.s3Region()).isEqualTo("us-west-2");
        assertThat(config.s3Bucket()).isEqualTo("test-bucket");
        assertThat(config.s3EndpointOverride()).isNull();
        assertThat(config.s3AccessKeyId()).isNull();
        assertThat(config.s3SecretAccessKey()).isNull();
    }

    @Test
    void testCreateWithAllOptions()
    {
        Map<String, String> options = new HashMap<>(REQUIRED_OPTIONS);
        options.put("s3-endpoint-override", "http://localhost:9000");
        options.put("s3-access-key-id", "test-access-key");
        options.put("s3-secret-access-key", "test-secret-key");

        S3ClientConfig config = S3ClientConfig.create(new CaseInsensitiveStringMap(options));

        assertThat(config.s3Region()).isEqualTo("us-west-2");
        assertThat(config.s3Bucket()).isEqualTo("test-bucket");
        assertThat(config.s3EndpointOverride()).isEqualTo("http://localhost:9000");
        assertThat(config.s3AccessKeyId()).isEqualTo("test-access-key");
        assertThat(config.s3SecretAccessKey()).isEqualTo("test-secret-key");
    }

    @Test
    void testMissingRegion()
    {
        Map<String, String> options = new HashMap<>();
        options.put("s3-bucket", "test-bucket");

        assertThatThrownBy(() -> S3ClientConfig.create(new CaseInsensitiveStringMap(options)))
            .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testMissingBucket()
    {
        Map<String, String> options = new HashMap<>();
        options.put("s3-region", "us-west-2");

        assertThatThrownBy(() -> S3ClientConfig.create(new CaseInsensitiveStringMap(options)))
            .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testCreateWithExplicitValues()
    {
        S3ClientConfig config = S3ClientConfig.create(
            "eu-west-1",
            "my-bucket",
            "http://minio:9000",
            "access123",
            "secret456"
        );

        assertThat(config.s3Region()).isEqualTo("eu-west-1");
        assertThat(config.s3Bucket()).isEqualTo("my-bucket");
        assertThat(config.s3EndpointOverride()).isEqualTo("http://minio:9000");
        assertThat(config.s3AccessKeyId()).isEqualTo("access123");
        assertThat(config.s3SecretAccessKey()).isEqualTo("secret456");
    }

    @Test
    void testCreateWithExplicitValuesNullOptional()
    {
        S3ClientConfig config = S3ClientConfig.create(
            "us-east-1",
            "bucket",
            null,
            null,
            null
        );

        assertThat(config.s3Region()).isEqualTo("us-east-1");
        assertThat(config.s3Bucket()).isEqualTo("bucket");
        assertThat(config.s3EndpointOverride()).isNull();
        assertThat(config.s3AccessKeyId()).isNull();
        assertThat(config.s3SecretAccessKey()).isNull();
    }
}
