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

package org.apache.cassandra.spark.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BuildInfoTest
{
    @Test
    public void testReaderUserAgent()
    {
        assertTrue(BuildInfo.READER_USER_AGENT.endsWith(" reader"));
        assertNotEquals("unknown", BuildInfo.getBuildVersion());
    }

    @Test
    public void testWriterUserAgent()
    {
        assertTrue(BuildInfo.WRITER_USER_AGENT.endsWith(" writer"));
        assertNotEquals("unknown", BuildInfo.getBuildVersion());
    }

    @Test
    public void testJavaVersionReturnsAValue()
    {
        assertNotNull(BuildInfo.javaSpecificationVersion());
    }

    @Test
    public void isAtLeastJava11()
    {
        assertFalse(BuildInfo.isAtLeastJava11(null));
        assertFalse(BuildInfo.isAtLeastJava11("0.9"));
        assertFalse(BuildInfo.isAtLeastJava11("1.1"));
        assertFalse(BuildInfo.isAtLeastJava11("1.2"));
        assertFalse(BuildInfo.isAtLeastJava11("1.3"));
        assertFalse(BuildInfo.isAtLeastJava11("1.4"));
        assertFalse(BuildInfo.isAtLeastJava11("1.5"));
        assertFalse(BuildInfo.isAtLeastJava11("1.6"));
        assertFalse(BuildInfo.isAtLeastJava11("1.7"));
        assertFalse(BuildInfo.isAtLeastJava11("1.8"));
        assertFalse(BuildInfo.isAtLeastJava11("9"));
        assertFalse(BuildInfo.isAtLeastJava11("10"));
        assertTrue(BuildInfo.isAtLeastJava11("11"));
        assertTrue(BuildInfo.isAtLeastJava11("12"));
        assertTrue(BuildInfo.isAtLeastJava11("13"));
        assertTrue(BuildInfo.isAtLeastJava11("14"));
        assertTrue(BuildInfo.isAtLeastJava11("15"));
        assertTrue(BuildInfo.isAtLeastJava11("16"));
        assertTrue(BuildInfo.isAtLeastJava11("17"));
        assertTrue(BuildInfo.isAtLeastJava11("18"));
        assertTrue(BuildInfo.isAtLeastJava11("19"));
        assertTrue(BuildInfo.isAtLeastJava11("20"));
        assertTrue(BuildInfo.isAtLeastJava11("21"));
    }
}
