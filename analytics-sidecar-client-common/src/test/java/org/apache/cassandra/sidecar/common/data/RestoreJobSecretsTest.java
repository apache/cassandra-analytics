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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestoreJobSecretsTest
{
    @Test
    void testIamModeCreatesRegionOnlyReadCredentials()
    {
        RestoreJobSecrets secrets = RestoreJobSecrets.iamMode("us-east-1", "us-west-2");
        assertThat(secrets.readCredentials().region()).isEqualTo("us-east-1");
        assertThat(secrets.readCredentials().accessKeyId()).isNull();
        assertThat(secrets.readCredentials().secretAccessKey()).isNull();
        assertThat(secrets.readCredentials().sessionToken()).isNull();
        assertThat(secrets.readCredentials().hasStaticCredentials()).isFalse();
    }

    @Test
    void testIamModeUsesSeparateReadAndWriteRegions()
    {
        RestoreJobSecrets secrets = RestoreJobSecrets.iamMode("read-region", "write-region");
        assertThat(secrets.readCredentials().region()).isEqualTo("read-region");
        assertThat(secrets.writeCredentials().region()).isEqualTo("write-region");
        assertThat(secrets.readCredentials().hasStaticCredentials()).isFalse();
        assertThat(secrets.writeCredentials().hasStaticCredentials()).isFalse();
    }

    @Test
    void testConstructorRequiresNonNullReadCredentials()
    {
        StorageCredentials write = StorageCredentials.builder().region("us-east-1").build();
        assertThatThrownBy(() -> new RestoreJobSecrets(null, write))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Read credentials must be supplied");
    }

    @Test
    void testConstructorRequiresNonNullWriteCredentials()
    {
        StorageCredentials read = StorageCredentials.builder().region("us-east-1").build();
        assertThatThrownBy(() -> new RestoreJobSecrets(read, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Write credentials must be supplied");
    }
}
