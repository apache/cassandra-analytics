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

package org.apache.cassandra.spark.data.backup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the Java-serialization contract that backs executor-side {@link BackupReader}
 * instantiation. The factory is captured into a Spark task closure and must round-trip cleanly
 * through {@link ObjectOutputStream}/{@link ObjectInputStream}.
 */
class BackupReaderFactorySerializationTest
{
    @Test
    void factoryLambdaSurvivesJavaSerialization() throws Exception
    {
        BackupReaderFactory factory = config -> new FakeBackupReader(config.s3Config(), "ser-test-bucket");

        BackupReaderFactory roundTripped = roundTrip(factory);

        assertThat(roundTripped).as("deserialized factory must not be null").isNotNull();
        BackupReader created = roundTripped.create(BackupReaderConfig.of(/* s3Config */ null));
        assertThat(created)
                .as("deserialized factory must continue producing the expected reader type")
                .isInstanceOf(FakeBackupReader.class);
        assertThat(created.bucket())
                .as("captured state inside the lambda must survive serialization")
                .isEqualTo("ser-test-bucket");
    }

    @Test
    void factoryFromStaticMethodReferenceSurvivesSerialization() throws Exception
    {
        // Method reference targeting a static helper. The captured target must remain resolvable
        // post-deserialization without holding a reference to the enclosing test instance.
        BackupReaderFactory factory = BackupReaderFactorySerializationTest::createFake;

        BackupReaderFactory roundTripped = roundTrip(factory);

        BackupReader created = roundTripped.create(BackupReaderConfig.of(null));
        assertThat(created.getClass()).isSameAs(FakeBackupReader.class);
    }

    private static FakeBackupReader createFake(BackupReaderConfig config)
    {
        return new FakeBackupReader(config.s3Config(), "static-ref-bucket");
    }

    private static BackupReaderFactory roundTrip(BackupReaderFactory factory) throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes))
        {
            out.writeObject(factory);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))
        {
            return (BackupReaderFactory) in.readObject();
        }
    }
}
