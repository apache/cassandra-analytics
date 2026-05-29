/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.data;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageCredentialsTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testHasStaticCredentialsReturnsTrueWhenAllFieldsPresent()
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .accessKeyId("key")
                                                     .secretAccessKey("secret")
                                                     .sessionToken("token")
                                                     .region("us-east-1")
                                                     .build();
        assertThat(creds.hasStaticCredentials()).isTrue();
    }

    @Test
    void testHasStaticCredentialsReturnsFalseForRegionOnlyIamMode()
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .region("us-east-1")
                                                     .build();
        assertThat(creds.hasStaticCredentials()).isFalse();
    }

    @Test
    void testHasStaticCredentialsReturnsFalseWhenAccessKeyIdMissing()
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .secretAccessKey("secret")
                                                     .sessionToken("token")
                                                     .region("us-east-1")
                                                     .build();
        assertThat(creds.hasStaticCredentials()).isFalse();
    }

    @Test
    void testHasStaticCredentialsReturnsFalseWhenSecretKeyMissing()
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .accessKeyId("key")
                                                     .sessionToken("token")
                                                     .region("us-east-1")
                                                     .build();
        assertThat(creds.hasStaticCredentials()).isFalse();
    }

    @Test
    void testHasStaticCredentialsReturnsFalseWhenSessionTokenMissing()
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .accessKeyId("key")
                                                     .secretAccessKey("secret")
                                                     .region("us-east-1")
                                                     .build();
        assertThat(creds.hasStaticCredentials()).isFalse();
    }

    @Test
    void testConstructorRequiresRegion()
    {
        assertThatThrownBy(() -> StorageCredentials.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("region must be supplied");
    }

    @Test
    void testSerializationRegionOnlyCredentialsOmitsNullKeyFields() throws JsonProcessingException
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .region("us-east-1")
                                                     .build();
        String json = MAPPER.writeValueAsString(creds);
        assertThat(json).contains("\"region\":\"us-east-1\"")
                        .doesNotContain("accessKeyId")
                        .doesNotContain("secretAccessKey")
                        .doesNotContain("sessionToken");
    }

    @Test
    void testSerializationFullCredentialsIncludesAllFields() throws JsonProcessingException
    {
        StorageCredentials creds = StorageCredentials.builder()
                                                     .accessKeyId("key")
                                                     .secretAccessKey("secret")
                                                     .sessionToken("token")
                                                     .region("us-east-1")
                                                     .build();
        String json = MAPPER.writeValueAsString(creds);
        assertThat(json).contains("\"accessKeyId\":\"key\"")
                        .contains("\"secretAccessKey\":\"secret\"")
                        .contains("\"sessionToken\":\"token\"")
                        .contains("\"region\":\"us-east-1\"");
    }

    @Test
    void testDeserializationRegionOnlyCredentials() throws JsonProcessingException
    {
        String json = "{\"region\":\"eu-west-1\"}";
        StorageCredentials creds = MAPPER.readValue(json, StorageCredentials.class);
        assertThat(creds.region()).isEqualTo("eu-west-1");
        assertThat(creds.accessKeyId()).isNull();
        assertThat(creds.secretAccessKey()).isNull();
        assertThat(creds.sessionToken()).isNull();
        assertThat(creds.hasStaticCredentials()).isFalse();
    }

    @Test
    void testRoundTripFullCredentials() throws JsonProcessingException
    {
        StorageCredentials original = StorageCredentials.builder()
                                                        .accessKeyId("key")
                                                        .secretAccessKey("secret")
                                                        .sessionToken("token")
                                                        .region("us-west-2")
                                                        .build();
        String json = MAPPER.writeValueAsString(original);
        StorageCredentials roundTripped = MAPPER.readValue(json, StorageCredentials.class);
        assertThat(roundTripped).isEqualTo(original);
        assertThat(roundTripped.hasStaticCredentials()).isTrue();
    }
}
