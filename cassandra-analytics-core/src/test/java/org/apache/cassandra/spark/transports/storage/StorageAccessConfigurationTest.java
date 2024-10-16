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

import static org.assertj.core.api.Assertions.assertThat;

class StorageAccessConfigurationTest
{
    @Test
    void testCopyWithNewCredentials()
    {
        StorageAccessConfiguration config1 = new StorageAccessConfiguration("writeRegion", "writeBucket",
                                                                            new StorageCredentials("access", "secret"));
        StorageAccessConfiguration config2 = config1.copyWithNewCredentials(new StorageCredentials("newAccess", "newSecret"));
        assertThat(config1).isNotSameAs(config2);
        assertThat(config1.bucket()).isEqualTo(config2.bucket());
        assertThat(config1.region()).isEqualTo(config2.region());
        assertThat(config1.storageCredentials()).isNotEqualTo(config2.storageCredentials());
    }

    @Test
    void testHashcodeAndEquals()
    {
        StorageAccessConfiguration config1 = new StorageAccessConfiguration("writeRegion", "writeBucket",
                                                                            new StorageCredentials("access", "secret"));
        StorageAccessConfiguration config2 = new StorageAccessConfiguration("writeRegion", "writeBucket",
                                                                            new StorageCredentials("access", "secret"));
        assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        assertThat(config1).isEqualTo(config2);

        config2 = config1.copyWithNewCredentials(new StorageCredentials("newAccess", "newSecret"));
        assertThat(config1.hashCode()).isNotEqualTo(config2.hashCode());
        assertThat(config1).isNotEqualTo(config2);
    }
}
