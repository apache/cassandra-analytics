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

package org.apache.cassandra.spark.data;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.spark.data.ClientConfig.SNAPSHOT_TTL_PATTERN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientConfigTests
{
    @Test
    void testPositiveSnapshotTTLPatterns()
    {
        assertTrue("2h".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("200s".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("20000ms".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("4d".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("60m".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("2e+9us".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("1.5e+9µs".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("2e+1.9ns".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue(".2e+9ns".matches(SNAPSHOT_TTL_PATTERN));
        assertTrue("2e+.9ns".matches(SNAPSHOT_TTL_PATTERN));
    }

    @Test
    void testNegativeSnapshotTTLPatterns()
    {
        assertFalse("2 h".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("200".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("ms".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse(".89".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("39xs".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("2+9ms".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("2e8µs".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("2+9ms".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse(".e+9ns".matches(SNAPSHOT_TTL_PATTERN));
        assertFalse("2e+.us".matches(SNAPSHOT_TTL_PATTERN));
    }
}
