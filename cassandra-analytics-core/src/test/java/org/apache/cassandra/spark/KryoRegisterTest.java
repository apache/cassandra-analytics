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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.spark.SparkConf;

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

    @Test
    void testSetupRegistersAllImplementedVersions()
    {
        SparkConf conf = new SparkConf();
        KryoRegister.setup(conf);

        List<String> expected = Arrays.stream(CassandraVersion.implementedVersions())
                                      .map(KryoRegister.KRYO_REGISTRATORS::get)
                                      .map(Class::getName)
                                      .collect(Collectors.toList());

        // setup() must register a registrator for every implemented (bundled) bridge version,
        // independent of spark.cassandra_analytics.cassandra.version, so serialization works for
        // whichever bridge the SSTable-version analyzer selects at runtime.
        assertThat(expected).isNotEmpty();
        List<String> registrators = Arrays.asList(conf.get("spark.kryo.registrator").split(","));
        assertThat(registrators).containsAll(expected);
    }

    @Test
    void testSetupDoesNotDependOnCassandraVersionConfig()
    {
        // Even with a cassandra.version that differs from the bridge that may be selected,
        // setup() registers all implemented versions and never throws based on that config.
        SparkConf conf = new SparkConf()
                         .set("spark.cassandra_analytics.cassandra.version", "5.0.0");
        assertThatNoException().isThrownBy(() -> KryoRegister.setup(conf));
        assertThat(conf.get("spark.serializer")).isEqualTo("org.apache.spark.serializer.KryoSerializer");
    }

    @Test
    void testSetupPreservesExistingRegistrators()
    {
        SparkConf conf = new SparkConf()
                         .set("spark.kryo.registrator", "com.example.CustomRegistrator");
        KryoRegister.setup(conf);

        List<String> registrators = Arrays.asList(conf.get("spark.kryo.registrator").split(","));
        assertThat(registrators).contains("com.example.CustomRegistrator");
        assertThat(registrators.get(0))
        .describedAs("pre-existing registrators should be kept first (deterministic order)")
        .isEqualTo("com.example.CustomRegistrator");
    }
}
