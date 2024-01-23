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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.cassandra.spark.data.ClientConfig.SNAPSHOT_TTL_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClientConfigTests
{
    @ParameterizedTest
    @ValueSource(strings = {"2h", "200s", "4d", "60m", "  60m", "50s ", " 32d "})
    void testPositiveSnapshotTTLPatterns(String input)
    {
        assertThat(input.trim()).matches(SNAPSHOT_TTL_PATTERN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "2 h", "200", "d", "6 0m", " h", "3.5h", ".8m", "4.d", "1e+7m"})
    void testNegativeSnapshotTTLPatterns(String input)
    {
        assertThat(input).doesNotMatch(SNAPSHOT_TTL_PATTERN);
    }

    @ParameterizedTest
    @ValueSource()
    void testValidTTLClearSnapshotStrategyParsing(boolean hasDeprecatedSnapshotOption, boolean clearSnapshot,
                                                  String option, String expectedSnapshotTTL)
    {
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(clientConfig.clearSnapshotStrategy()).thenCallRealMethod();
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(hasDeprecatedSnapshotOption, clearSnapshot, option);
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isFalse();
        assertThat(clearSnapshotStrategy.hasTTL()).isTrue();
        assertThat(clearSnapshotStrategy.ttl()).isEqualTo(expectedSnapshotTTL);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "2 h", "200", "d", "6 0m", " h", "3.5h", ".8m", "4.d", "1e+7m"})
    void testValidNoOpClearSnapshotStrategyParsing(boolean hasDeprecatedSnapshotOption, boolean clearSnapshot,
                                                   String option)
    {
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(clientConfig.clearSnapshotStrategy()).thenCallRealMethod();
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(hasDeprecatedSnapshotOption, clearSnapshot, option);
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isFalse();
        assertThat(clearSnapshotStrategy.hasTTL()).isFalse();
        assertThat(clearSnapshotStrategy.ttl()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "2 h", "200", "d", "6 0m", " h", "3.5h", ".8m", "4.d", "1e+7m"})
    void testValidOnCompletionClearSnapshotStrategyParsing(boolean hasDeprecatedSnapshotOption, boolean clearSnapshot,
                                                           String option)
    {
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(clientConfig.clearSnapshotStrategy()).thenCallRealMethod();
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(hasDeprecatedSnapshotOption, clearSnapshot, option);
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isTrue();
        assertThat(clearSnapshotStrategy.hasTTL()).isFalse();
        assertThat(clearSnapshotStrategy.ttl()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "2 h", "200", "d", "6 0m", " h", "3.5h", ".8m", "4.d", "1e+7m"})
    void testValidOnCompletionOrTTLClearSnapshotStrategyParsing(boolean hasDeprecatedSnapshotOption,
                                                                boolean clearSnapshot, String option,
                                                                String expectedSnapshotTTL)
    {
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(clientConfig.clearSnapshotStrategy()).thenCallRealMethod();
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(hasDeprecatedSnapshotOption, clearSnapshot, option);
        assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isTrue();
        assertThat(clearSnapshotStrategy.hasTTL()).isTrue();
        assertThat(clearSnapshotStrategy.ttl()).isEqualTo(expectedSnapshotTTL);
    }
}
