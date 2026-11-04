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

package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionerTest
{
    @ParameterizedTest
    @EnumSource(Partitioner.class)
    void hashDoesNotModifyTheKey(Partitioner partitioner)
    {
        // A key that starts after the beginning of its backing array, as a slice of a composite key does
        ByteBuffer buffer = ByteBuffer.wrap("__partition-key".getBytes(StandardCharsets.UTF_8));
        buffer.position(2);
        ByteBuffer key = buffer.slice();
        ByteBuffer expected = key.duplicate();

        BigInteger token = partitioner.hash(key);

        assertThat(key.position()).isZero();
        assertThat(key).isEqualTo(expected);
        assertThat(partitioner.hash(key)).isEqualTo(token);
        assertThat(token).isBetween(partitioner.minToken(), partitioner.maxToken());
    }
}
