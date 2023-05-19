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

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.jetbrains.annotations.NotNull;

public final class SSTables
{
    private enum ComponentType
    {
        VERSION,
        GENERATION,
        FORMAT,
        FILE_COMPONENT
    }

    private static final String FILE_PATH_SEPARATOR = "-";
    private static final Map<String, CassandraVersionFeatures> SS_TABLE_VERSIONS = ImmutableMap
            .<String, CassandraVersionFeatures>builder()
            .put("ma", new CassandraVersionFeatures(30,  0, null))
            .put("mb", new CassandraVersionFeatures(30,  7, null))
            .put("mc", new CassandraVersionFeatures(30,  8, null))
            .put("md", new CassandraVersionFeatures(30, 18, null))
            .put("me", new CassandraVersionFeatures(30, 25, null))
            .put("mf", new CassandraVersionFeatures(30, 18, null))
            .put("na", new CassandraVersionFeatures(40,  0, null))
            .put("nb", new CassandraVersionFeatures(40,  0, null))
            .build();

    private SSTables()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private static LinkedHashMap<ComponentType, String> parse(@NotNull Path sstable)
    {
        String name = sstable.getFileName().toString();
        StringTokenizer st = new StringTokenizer(name, FILE_PATH_SEPARATOR);
        LinkedHashMap<ComponentType, String> reverseOrderComponents = Maps.newLinkedHashMap();

        // Read tokens backwards to determine version
        Deque<String> tokenStack = new ArrayDeque<>();
        while (st.hasMoreTokens())
        {
            tokenStack.push(st.nextToken());
        }

        reverseOrderComponents.put(ComponentType.FILE_COMPONENT, tokenStack.pop());

        // In pre 2.2 tables, the next token is the generation but post 2.2 it's the format. If all
        // the chars in this token are numeric, we can assume a generation, as Cassandra does
        // (see o.a.c.io.sstable.Descriptor::fromFilename). Otherwise, interpret as the format
        // identifier and so skip over it and read the generation from the next token.
        String nextToken = tokenStack.pop();
        if (!isNumeric(nextToken))
        {
            reverseOrderComponents.put(ComponentType.FORMAT, nextToken);
            nextToken = tokenStack.pop();
        }
        reverseOrderComponents.put(ComponentType.GENERATION, nextToken);

        String version = tokenStack.pop();
        reverseOrderComponents.put(ComponentType.VERSION, version);

        // Finally, reverse the component map to put it in forward order
        List<ComponentType> keys = new ArrayList<>(reverseOrderComponents.keySet());
        Collections.reverse(keys);
        LinkedHashMap<ComponentType, String> components = Maps.newLinkedHashMap();
        keys.forEach(componentType -> components.put(componentType, reverseOrderComponents.get(componentType)));
        return components;
    }

    /**
     * Replaces guava's `com.google.common.base.CharMatcher.digit().matchesAllOf` to remove the dependency
     * of guava just for the test
     *
     * @param string the input string
     * @return true if only 0-9 contained in the string, false otherwise
     */
    private static boolean isNumeric(String string)
    {
        for (int index = 0; index < string.length(); index++)
        {
            char character = string.charAt(index);
            if (character < '0' || '9' < character)
            {
                return false;
            }
        }
        return true;
    }

    public static CassandraVersionFeatures cassandraVersionFromTable(@NotNull Path sstable)
    {
        String version = parse(sstable).get(ComponentType.VERSION);
        if (!SS_TABLE_VERSIONS.containsKey(version))
        {
            throw new UnsupportedOperationException("SSTable version: " + version + " is not supported");
        }

        return SS_TABLE_VERSIONS.get(version);
    }
}
