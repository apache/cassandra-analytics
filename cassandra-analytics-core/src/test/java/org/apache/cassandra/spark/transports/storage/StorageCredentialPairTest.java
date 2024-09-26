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

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;

import static org.assertj.core.api.Assertions.assertThat;

class StorageCredentialPairTest
{
    @Test
    void testToRestoreJobSecrets()
    {
        StorageCredentialPair pair = makePair(1);
        RestoreJobSecrets secrets = pair.toRestoreJobSecrets();
        assertThat(secrets.readCredentials().accessKeyId()).isEqualTo("readAccessKey1");
        assertThat(secrets.readCredentials().secretAccessKey()).isEqualTo("readSecretKey1");
        assertThat(secrets.readCredentials().sessionToken()).isEqualTo("readSession1");
        assertThat(secrets.readCredentials().region()).isEqualTo("readRegion1");
        assertThat(secrets.writeCredentials().accessKeyId()).isEqualTo("writeAccessKey1");
        assertThat(secrets.writeCredentials().secretAccessKey()).isEqualTo("writeSecretKey1");
        assertThat(secrets.writeCredentials().sessionToken()).isEqualTo("writeSession1");
        assertThat(secrets.writeCredentials().region()).isEqualTo("writeRegion1");
    }

    @Test
    void testHashcodeAndEquals()
    {
        StorageCredentialPair pair1 = makePair(1);
        StorageCredentialPair pair2 = makePair(1);
        assertThat(pair1.hashCode()).isEqualTo(pair2.hashCode());
        assertThat(pair1).isEqualTo(pair2);

        pair2 = makePair(2);
        assertThat(pair1.hashCode()).isNotEqualTo(pair2.hashCode());
        assertThat(pair1).isNotEqualTo(pair2);
    }

    private StorageCredentialPair makePair(int id)
    {
        return new StorageCredentialPair("writeRegion" + id,
                                         new StorageCredentials("writeAccessKey" + id,
                                                                "writeSecretKey" + id,
                                                                "writeSession" + id),
                                         "readRegion" + id,
                                         new StorageCredentials("readAccessKey" + id,
                                                                "readSecretKey" + id,
                                                                "readSession" + id));
    }
}
