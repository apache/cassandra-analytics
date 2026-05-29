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

package org.apache.cassandra.spark.bulkwriter;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CassandraBulkSourceRelation#buildRestoreJobSecrets(StorageCredentialPair, String)}.
 */
class BuildRestoreJobSecretsTest
{
    @Test
    void testNullCredentialTypeReturnsStaticSecrets()
    {
        StorageCredentialPair pair = staticPair();
        RestoreJobSecrets secrets = CassandraBulkSourceRelation.buildRestoreJobSecrets(pair, null);
        assertThat(secrets.readCredentials().accessKeyId()).isEqualTo("readKey");
        assertThat(secrets.readCredentials().secretAccessKey()).isEqualTo("readSecret");
        assertThat(secrets.readCredentials().sessionToken()).isEqualTo("readToken");
        assertThat(secrets.readCredentials().region()).isEqualTo("us-east-1");
        assertThat(secrets.writeCredentials().accessKeyId()).isEqualTo("writeKey");
        assertThat(secrets.writeCredentials().secretAccessKey()).isEqualTo("writeSecret");
        assertThat(secrets.writeCredentials().sessionToken()).isEqualTo("writeToken");
        assertThat(secrets.writeCredentials().region()).isEqualTo("eu-west-1");
    }

    @Test
    void testIamCredentialTypeReturnsRegionOnlySecrets()
    {
        StorageCredentialPair pair = StorageCredentialPair.iamPair("us-east-1", "eu-west-1");
        RestoreJobSecrets secrets = CassandraBulkSourceRelation.buildRestoreJobSecrets(pair, "IAM");
        assertThat(secrets.readCredentials().region()).isEqualTo("us-east-1");
        assertThat(secrets.readCredentials().accessKeyId()).isNull();
        assertThat(secrets.readCredentials().secretAccessKey()).isNull();
        assertThat(secrets.readCredentials().sessionToken()).isNull();
        assertThat(secrets.writeCredentials().region()).isEqualTo("eu-west-1");
        assertThat(secrets.writeCredentials().accessKeyId()).isNull();
        assertThat(secrets.writeCredentials().secretAccessKey()).isNull();
        assertThat(secrets.writeCredentials().sessionToken()).isNull();
    }

    private StorageCredentialPair staticPair()
    {
        return new StorageCredentialPair(
        "eu-west-1",
        new StorageCredentials("writeKey", "writeSecret", "writeToken"),
        "us-east-1",
        new StorageCredentials("readKey", "readSecret", "readToken"));
    }
}
