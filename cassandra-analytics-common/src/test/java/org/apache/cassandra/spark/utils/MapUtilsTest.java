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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MapUtilsTest
{
    @Test
    void firstEntry()
    {
        Map<String, String> m = new HashMap<>();
        m.put("foo", "bar");
        assertThat(MapUtils.firstEntry(m).getKey()).isEqualTo("foo");
        assertThat(MapUtils.firstEntry(m).getValue()).isEqualTo("bar");
        m.put("X", "Y");
        String key = MapUtils.firstEntry(m).getKey();
        String val = MapUtils.firstEntry(m).getValue();
        assertThat(key).isIn("foo", "X");
        assertThat(val).isIn("bar", "Y");
        assertThat(m.get(key)).isEqualTo(val);
    }

    @Test
    void firstEntryFromEmptyMap()
    {
        assertThatThrownBy(() -> MapUtils.firstEntry(Collections.emptyMap()))
        .isExactlyInstanceOf(NoSuchElementException.class)
        .hasMessage("No entry to return from an empty map");
    }
}
