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

package org.apache.cassandra.spark;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for KryoRegister
 */
public class KryoRegisterTest
{
    @Test
    void testValidateKryoRegistratorExistsForFourZero()
    {
        assertThatNoException()
        .describedAs("FOURZERO should have a Kryo registrator")
        .isThrownBy(() -> KryoRegister.validateKryoRegistratorExists(CassandraVersion.FOURZERO, "4.0.0"));
    }

    @Test
    void testValidateKryoRegistratorExistsForFourOne()
    {
        assertThatNoException()
        .describedAs("FOURONE should have a Kryo registrator")
        .isThrownBy(() -> KryoRegister.validateKryoRegistratorExists(CassandraVersion.FOURONE, "4.1.0"));
    }

    @Test
    void testValidateKryoRegistratorExistsForFiveZero()
    {
        assertThatNoException()
        .describedAs("FIVEZERO should have a Kryo registrator")
        .isThrownBy(() -> KryoRegister.validateKryoRegistratorExists(CassandraVersion.FIVEZERO, "5.0.0"));
    }

    @Test
    void testValidateKryoRegistratorMissingForThreeZero()
    {
        assertThatThrownBy(() -> KryoRegister.validateKryoRegistratorExists(CassandraVersion.THREEZERO, "3.0.0"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No Kryo registrator configured for bridge version THREEZERO")
        .hasMessageContaining("Cluster Cassandra version: 3.0.0")
        .hasMessageContaining("Available Kryo registrators:")
        .hasMessageContaining("FOURZERO")
        .hasMessageContaining("FOURONE")
        .hasMessageContaining("FIVEZERO")
        // should mention config param to update
        .hasMessageContaining("spark.cassandra_analytics.cassandra.version");
    }

    @Test
    void testKryoRegistratorClassesAreCorrect()
    {
        assertThat(KryoRegister.KRYO_REGISTRATORS.get(CassandraVersion.FOURZERO))
        .isEqualTo(KryoRegister.V40.class);

        assertThat(KryoRegister.KRYO_REGISTRATORS.get(CassandraVersion.FOURONE))
        .isEqualTo(KryoRegister.V41.class);

        assertThat(KryoRegister.KRYO_REGISTRATORS.get(CassandraVersion.FIVEZERO))
        .isEqualTo(KryoRegister.V50.class);
    }

    @Test
    void testValidateWithNullCassandraVersionString()
    {
        // Should not throw - clusterCassandraVersion is optional for error message context
        assertThatNoException()
        .describedAs("Validation should work with null cassandraVersion string")
        .isThrownBy(() -> KryoRegister.validateKryoRegistratorExists(CassandraVersion.FOURZERO, null));
    }
}
