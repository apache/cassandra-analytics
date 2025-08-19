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
    public void isAtLeastJava11()
    {
        assertThat(BuildInfo.isAtLeastJava11(null)).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("0.9")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.1")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.2")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.3")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.4")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.5")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.6")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.7")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("1.8")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("9")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("10")).isFalse();
        assertThat(BuildInfo.isAtLeastJava11("11")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("12")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("13")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("14")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("15")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("16")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("17")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("18")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("19")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("20")).isTrue();
        assertThat(BuildInfo.isAtLeastJava11("21")).isTrue();
    }
}
