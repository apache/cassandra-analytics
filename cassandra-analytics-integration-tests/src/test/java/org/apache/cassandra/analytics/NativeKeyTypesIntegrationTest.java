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

package org.apache.cassandra.analytics;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

/**
 * Bulk writes partition and clustering keys of every native type that Cassandra allows in a primary key, which is
 * every native type except {@code counter} and {@code duration}
 */
class NativeKeyTypesIntegrationTest extends KeyTypesIntegrationTestBase
{
    private static final long TIMESTAMP = 1_700_000_000_000L;

    @Override
    List<KeyType> keyTypes()
    {
        return Arrays.asList(
        new KeyType("ascii", "ascii", i -> String.format("'ascii_%d'", i)),
        new KeyType("bigint", "bigint", i -> String.valueOf((i - 3) * 1_000_000_007L)),
        new KeyType("blob", "blob", i -> String.format("0x%02x00ff", i)),
        new KeyType("boolean", "boolean", 2, i -> i % 2 == 0 ? "false" : "true"),
        new KeyType("date", "date", i -> String.format("'2024-02-%02d'", i + 1)),
        // Spark reads decimals with a scale of 19, and the scale is part of the serialized key
        new KeyType("decimal", "decimal", i -> String.format("%d.1250000000000000000", i - 3)),
        new KeyType("double", "double", i -> String.format("%d.5", i - 3)),
        new KeyType("float", "float", i -> String.format("%d.25", i - 3)),
        new KeyType("inet", "inet", i -> i % 2 == 0 ? String.format("'10.0.%d.1'", i) : String.format("'2001:db8::%d'", i)),
        new KeyType("int", "int", i -> String.valueOf((i - 3) * 1000)),
        new KeyType("smallint", "smallint", i -> String.valueOf((i - 3) * 100)),
        new KeyType("text", "text", i -> String.format("'text_%d_ü'", i)),
        new KeyType("time", "time", i -> String.format("'08:12:%02d.123456789'", i)),
        new KeyType("timestamp", "timestamp", i -> String.valueOf(TIMESTAMP + i * 1000L)),
        new KeyType("timeuuid", "timeuuid", i -> UUIDs.startOf(TIMESTAMP + i).toString()),
        new KeyType("tinyint", "tinyint", i -> String.valueOf(i - 3)),
        new KeyType("uuid", "uuid", i -> new UUID(i, -i).toString()),
        new KeyType("varchar", "varchar", i -> String.format("'varchar_%d'", i)),
        new KeyType("varint", "varint", i -> BigInteger.valueOf(i - 3).multiply(BigInteger.TEN.pow(20)).toString()));
    }
}
