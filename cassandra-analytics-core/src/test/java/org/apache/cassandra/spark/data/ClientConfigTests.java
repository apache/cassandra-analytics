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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.cassandra.spark.data.ClientConfig.SNAPSHOT_TTL_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientConfigTests
{
    public static final Map<String, String> REQUIRED_CLIENT_CONFIG_OPTIONS = ImmutableMap.of(
    "keyspace", "big-data",
    "table", "customers",
    "sidecar_instances", "localhost");

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
    @CsvSource({"false,false,tTL 10h,10h", "true,false,  TTL   5d  ,5d", "true,true,  Ttl 2m  ,2m", "false,false,noop,",
                "true,false,NoOp,", "true,true,  Noop ,", "false,false,noop 50m,", "false,false,onCompletion,",
                "true,false,OnCoMpLeTiOn,", "true,true,  ONCOMPLETION ,", "false,false,OnCoMpLeTiOn 5h,",
                "false,false,onCompletionOrTTL 200m, 200m", "true,false,oNcOmPlEtIoNoRtTL   0560m,0560m",
                "true,true,  ONCOMPLETIONORTTL  3d, 3d"})
    void testValidClearSnapshotStrategyParsing(boolean hasDeprecatedSnapshotOption, boolean clearSnapshot,
                                               String option, String expectedSnapshotTTL)
    {
        final Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        options.put("clearsnapshotstrategy", option);
        ClientConfig clientConfig = ClientConfig.create(options);
        ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy
        = clientConfig.parseClearSnapshotStrategy(hasDeprecatedSnapshotOption, clearSnapshot, option);
        validateStrategy(clearSnapshotStrategy, expectedSnapshotTTL);
    }

    @ParameterizedTest
    @CsvSource({"delete 10h", "ttl5d", "Ttl 2ms", "TTL", "No Op", "on Completion", "ON COMPLETION 3d",
                "onCompletionOrTTL ", "oN cOmPlEtIoNoRtTL 560m"})
    void testInValidClearSnapshotStrategyParsing(String option)
    {
        final Map<String, String> options = new HashMap<>(REQUIRED_CLIENT_CONFIG_OPTIONS);
        options.put("clearsnapshotstrategy", option);
        assertThatThrownBy(() -> {
            ClientConfig clientConfig = ClientConfig.create(options);
            clientConfig.parseClearSnapshotStrategy(false, false, option);
        })
        .isInstanceOf(IllegalArgumentException.class);
    }

    private void validateStrategy(ClientConfig.ClearSnapshotStrategy clearSnapshotStrategy, String expectedSnapshotTTL)
    {
        if (clearSnapshotStrategy instanceof ClientConfig.ClearSnapshotStrategy.TTL)
        {
            assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isFalse();
            assertThat(clearSnapshotStrategy.hasTTL()).isTrue();
            assertThat(clearSnapshotStrategy.ttl()).isEqualTo(expectedSnapshotTTL);
        }
        else if (clearSnapshotStrategy instanceof ClientConfig.ClearSnapshotStrategy.OnCompletionOrTTL)
        {
            assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isTrue();
            assertThat(clearSnapshotStrategy.hasTTL()).isTrue();
            assertThat(clearSnapshotStrategy.ttl()).isEqualTo(expectedSnapshotTTL);
        }
        else if (clearSnapshotStrategy instanceof ClientConfig.ClearSnapshotStrategy.OnCompletion)
        {
            assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isTrue();
            assertThat(clearSnapshotStrategy.hasTTL()).isFalse();
            assertThat(clearSnapshotStrategy.ttl()).isNull();
        }
        else
        {
            assertThat(clearSnapshotStrategy.shouldClearOnCompletion()).isFalse();
            assertThat(clearSnapshotStrategy.hasTTL()).isFalse();
            assertThat(clearSnapshotStrategy.ttl()).isNull();
        }
    }
}
