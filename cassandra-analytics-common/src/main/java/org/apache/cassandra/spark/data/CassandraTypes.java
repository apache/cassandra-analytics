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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import com.esotericsoftware.kryo.io.Input;

public abstract class CassandraTypes
{
    public static final Pattern COLLECTION_PATTERN = Pattern.compile("^(set|list|map|tuple)<(.+)>$", Pattern.CASE_INSENSITIVE);
    public static final Pattern FROZEN_PATTERN = Pattern.compile("^frozen<(.*)>$", Pattern.CASE_INSENSITIVE);

    private final UDTs udts = new UDTs();

    /**
     * Returns the quoted identifier, if the {@code identifier} has mixed case or if the {@code identifier}
     * is a reserved word.
     *
     * @param identifier the identifier
     * @return the quoted identifier when the input is mixed case or a reserved word, the original input otherwise
     */
    public abstract String maybeQuoteIdentifier(String identifier);

    public static boolean isAlreadyQuoted(String identifier)
    {
        if (identifier != null && identifier.length() > 1)
        {
            return identifier.charAt(0) == '"' &&
                   identifier.charAt(identifier.length() - 1) == '"';
        }
        return false;
    }

    public abstract CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input);

    public List<CqlField.NativeType> allTypes()
    {
        return Arrays.asList(ascii(), bigint(), blob(), bool(), counter(), date(), decimal(), aDouble(),
                             duration(), empty(), aFloat(), inet(), aInt(), smallint(), text(), time(),
                             timestamp(), timeuuid(), tinyint(), uuid(), varchar(), varint());
    }

    public abstract Map<String, ? extends CqlField.NativeType> nativeTypeNames();

    public CqlField.NativeType nativeType(String name)
    {
        return nativeTypeNames().get(name.toLowerCase());
    }

    public List<CqlField.NativeType> supportedTypes()
    {
        return allTypes().stream().filter(CqlField.NativeType::isSupported).collect(Collectors.toList());
    }

    // Native

    public abstract CqlField.NativeType ascii();

    public abstract CqlField.NativeType blob();

    public abstract CqlField.NativeType bool();

    public abstract CqlField.NativeType counter();

    public abstract CqlField.NativeType bigint();

    public abstract CqlField.NativeType date();

    public abstract CqlField.NativeType decimal();

    public abstract CqlField.NativeType aDouble();

    public abstract CqlField.NativeType duration();

    public abstract CqlField.NativeType empty();

    public abstract CqlField.NativeType aFloat();

    public abstract CqlField.NativeType inet();

    public abstract CqlField.NativeType aInt();

    public abstract CqlField.NativeType smallint();

    public abstract CqlField.NativeType text();

    public abstract CqlField.NativeType time();

    public abstract CqlField.NativeType timestamp();

    public abstract CqlField.NativeType timeuuid();

    public abstract CqlField.NativeType tinyint();

    public abstract CqlField.NativeType uuid();

    public abstract CqlField.NativeType varchar();

    public abstract CqlField.NativeType varint();

    // Complex

    public abstract CqlField.CqlType collection(String name, CqlField.CqlType... types);

    public abstract CqlField.CqlList list(CqlField.CqlType type);

    public abstract CqlField.CqlSet set(CqlField.CqlType type);

    public abstract CqlField.CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType);

    public abstract CqlField.CqlTuple tuple(CqlField.CqlType... types);

    public abstract CqlField.CqlType frozen(CqlField.CqlType type);

    public abstract CqlField.CqlUdtBuilder udt(String keyspace, String name);

    public CqlField.CqlType parseType(String type)
    {
        return parseType(type, Collections.emptyMap());
    }

    public CqlField.CqlType parseType(final String keyspace, final String type)
    {
        return parseType(type, udts.ofKeyspace(keyspace));
    }

    public void updateUDTs(String keyspace, CqlField.CqlUdt udt)
    {
        udts.put(keyspace, udt);
    }

    private static class UDTs
    {
        Map<String, Map<String, CqlField.CqlUdt>> udtByKeyspaceByTypeName = new ConcurrentHashMap<>();

        void put(String keyspace, CqlField.CqlUdt udt)
        {
            Map<String, CqlField.CqlUdt> udtByName = udtByKeyspaceByTypeName.computeIfAbsent(keyspace, x -> new ConcurrentHashMap<>());
            udtByName.put(udt.cqlName(), udt);
        }

        Map<String, CqlField.CqlUdt> ofKeyspace(String keyspace)
        {
            return udtByKeyspaceByTypeName.getOrDefault(keyspace, Collections.emptyMap());
        }
    }

    public CqlField.CqlType parseType(String type, Map<String, CqlField.CqlUdt> udts)
    {
        if (type == null || type.length() == 0)
        {
            return null;
        }
        Matcher collectionMatcher = COLLECTION_PATTERN.matcher(type);
        if (collectionMatcher.find())
        {
            // CQL collection
            String[] types = splitInnerTypes(collectionMatcher.group(2));
            return collection(collectionMatcher.group(1), Stream.of(types)
                                                                .map(collectionType -> parseType(collectionType, udts))
                                                                .toArray(CqlField.CqlType[]::new));
        }
        Matcher frozenMatcher = FROZEN_PATTERN.matcher(type);
        if (frozenMatcher.find())
        {
            // Frozen collections
            return frozen(parseType(frozenMatcher.group(1), udts));
        }

        if (udts.containsKey(type))
        {
            // User-defined type
            return udts.get(type);
        }

        // Native CQL 3 type
        return nativeType(type);
    }

    @VisibleForTesting
    public static String[] splitInnerTypes(String str)
    {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parentheses = 0;
        for (int index = 0; index < str.length(); index++)
        {
            char character = str.charAt(index);
            switch (character)
            {
                case ' ':
                    if (parentheses == 0)
                    {
                        continue;
                    }
                    break;
                case ',':
                    if (parentheses == 0)
                    {
                        if (current.length() > 0)
                        {
                            result.add(current.toString());
                            current = new StringBuilder();
                        }
                        continue;
                    }
                    break;
                case '<':
                    parentheses++;
                    break;
                case '>':
                    parentheses--;
                    break;
                default:
                    // Do nothing
            }
            current.append(character);
        }

        if (current.length() > 0 || result.isEmpty())
        {
            result.add(current.toString());
        }

        return result.toArray(new String[0]);
    }
}
