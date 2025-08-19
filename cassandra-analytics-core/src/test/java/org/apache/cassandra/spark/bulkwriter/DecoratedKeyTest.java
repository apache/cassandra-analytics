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

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DecoratedKeyTest
{
    @Test
    public void compareToBytesDifferent()
    {
        DecoratedKey key1 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a', 'a'}));
        DecoratedKey key2 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a', 'z'}));
        assertThat(key2.compareTo(key1)).isNotEqualTo(0);
    }

    @Test
    public void compareToBytesDifferentLengths()
    {
        DecoratedKey key1 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a'}));
        DecoratedKey key2 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a', 'a'}));
        assertThat(key2.compareTo(key1)).isNotEqualTo(0);
    }

    @Test
    public void compareToTokensDifferent()
    {
        DecoratedKey key1 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a'}));
        DecoratedKey key2 = new DecoratedKey(BigInteger.valueOf(2), ByteBuffer.wrap(new byte[]{'a'}));
        assertThat(key1.compareTo(key2)).isNotEqualTo(0);
    }

    @Test
    public void equals()
    {
        DecoratedKey key1 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a'}));
        DecoratedKey key2 = new DecoratedKey(BigInteger.valueOf(1), ByteBuffer.wrap(new byte[]{'a'}));
        assertThat(key1).isEqualTo(key2);
    }
}
