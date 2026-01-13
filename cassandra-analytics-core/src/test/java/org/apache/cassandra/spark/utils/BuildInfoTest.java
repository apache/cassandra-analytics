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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class BuildInfoTest
{
    @Test
    public void testReaderUserAgent()
    {
        assertThat(BuildInfo.READER_USER_AGENT).endsWith(" reader");
        assertThat(BuildInfo.getBuildVersion()).isNotEqualTo("unknown");
    }

    @Test
    public void testWriterUserAgent()
    {
        assertThat(BuildInfo.WRITER_USER_AGENT).endsWith(" writer");
        assertThat(BuildInfo.getBuildVersion()).isNotEqualTo("unknown");

        assertThat(BuildInfo.WRITER_S3_USER_AGENT).endsWith(" writer-s3");
        assertThat(BuildInfo.getBuildVersion()).isNotEqualTo("unknown");
    }

    @Test
    public void testJavaVersionReturnsAValue()
    {
        assertThat(BuildInfo.javaSpecificationVersion()).isNotNull();
    }

    @Test
    public void testIsAtLeastJavaVersionWithNullInput()
    {
        assertThat(BuildInfo.isAtLeastJava11(null)).isFalse();
        assertThat(BuildInfo.isAtLeastJava17(null)).isFalse();
    }

    @ParameterizedTest(name = "{index} => Java version {0}")
    @ValueSource(strings = { "0.9", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8", "9", "10" })
    public void isNotAtLeastJava11(String version)
    {
        assertThat(BuildInfo.isAtLeastJava11(version)).isFalse();
    }

    @ParameterizedTest(name = "{index} => Java version {0}")
    @ValueSource(strings = { "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25" })
    public void isAtLeastJava11(String version)
    {
        assertThat(BuildInfo.isAtLeastJava11(version)).isTrue();
    }

    @ParameterizedTest(name = "{index} => Java version {0}")
    @ValueSource(strings = { "0.9", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8", "9", "10",
                             "11", "12", "13", "14", "15", "16" })
    public void isNotAtLeastJava17(String version)
    {
        assertThat(BuildInfo.isAtLeastJava17(version)).isFalse();
    }

    @ParameterizedTest(name = "{index} => Java version {0}")
    @ValueSource(strings = { "17", "18", "19", "20", "21", "22", "23", "24", "25" })
    public void isAtLeastJava17(String version)
    {
        assertThat(BuildInfo.isAtLeastJava17(version)).isTrue();
    }
}
