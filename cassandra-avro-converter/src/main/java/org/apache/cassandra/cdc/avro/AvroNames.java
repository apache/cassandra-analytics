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

package org.apache.cassandra.cdc.avro;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Avro names must match [A-Za-z_][A-Za-z0-9_]*, but quoted CQL identifiers can contain any character.
 */
public final class AvroNames
{
    private AvroNames()
    {
    }

    /**
     * @return the name with every illegal character replaced by an underscore. A leading digit is prefixed with an
     * underscore, and an empty name becomes an underscore.
     */
    public static String sanitize(String name)
    {
        StringBuilder sanitized = new StringBuilder(name.length() + 1);
        for (int i = 0; i < name.length(); i++)
        {
            char c = name.charAt(i);
            boolean digit = c >= '0' && c <= '9';
            if (i == 0 && digit)
            {
                sanitized.append('_');
            }
            sanitized.append(digit || c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ? c : '_');
        }
        return sanitized.length() == 0 ? "_" : sanitized.toString();
    }

    /**
     * @return whether the name can be used in an Avro schema as it is
     */
    public static boolean isValid(String name)
    {
        return sanitize(name).equals(name);
    }

    /**
     * Assigns a distinct Avro name to each CQL name that is used in the same place of a schema, for example the
     * columns of a table or the fields of a UDT. A name that is valid as it is never changes. A sanitized name that
     * collides with another one gets a number appended, starting at 2.
     */
    public static final class Scope
    {
        private final Map<Object, String> names = new HashMap<>();
        private final Set<String> taken = new HashSet<>();

        /**
         * @param originals CQL names by id, in the order in which they claim names. Names that are valid claim theirs
         *                  first, so a sanitized name can never take one from them.
         */
        public Scope(Map<?, String> originals)
        {
            originals.forEach((id, original) -> {
                if (isValid(original))
                {
                    avroName(id, original);
                }
            });
            originals.forEach(this::avroName);
        }

        /**
         * @param id       identifies the element, as two elements can have the same CQL name
         * @param original CQL name of the element
         * @return the Avro name of the element, which is the same every time it is asked for
         */
        public String avroName(Object id, String original)
        {
            return names.computeIfAbsent(id, key -> {
                String base = sanitize(original);
                String candidate = base;
                for (int suffix = 2; !taken.add(candidate); suffix++)
                {
                    candidate = base + suffix;
                }
                return candidate;
            });
        }
    }
}
