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

/**
 * Avro names must match [A-Za-z_][A-Za-z0-9_]*, but quoted CQL identifiers can contain any character.
 */
public final class AvroNames
{
    private AvroNames()
    {
    }

    /**
     * Escapes every character that is illegal in an Avro name, and a leading digit, as {@code _uXXXX}, where
     * {@code XXXX} is the hexadecimal value of the UTF-16 char. For example, {@code my-col} becomes {@code my_u002dcol}.
     * Names that are valid are returned as they are.
     * <p>
     * The Avro name of an identifier depends only on the identifier, so it does not change when other columns, fields or
     * types are added or dropped. A valid name that looks like an escaped one, such as {@code my_u002dcol}, has the same
     * Avro name as the name it escapes; Avro rejects such duplicate fields when the schema is built.
     *
     * @return the Avro name for the CQL identifier
     */
    public static String escape(String name)
    {
        StringBuilder escaped = null;
        for (int i = 0; i < name.length(); i++)
        {
            char c = name.charAt(i);
            boolean valid = c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9');
            if (!valid && escaped == null)
            {
                escaped = new StringBuilder(name.length() + 8).append(name, 0, i);
            }
            if (escaped != null)
            {
                if (valid)
                {
                    escaped.append(c);
                }
                else
                {
                    escaped.append(String.format("_u%04x", (int) c));
                }
            }
        }
        return escaped == null ? name : escaped.toString();
    }

    /**
     * Escapes each dot-separated segment of a namespace, such as {@code <prefix>.<keyspace>}. See {@link #escape}.
     *
     * @return the Avro namespace, or null if the namespace is null
     */
    public static String escapeNamespace(String namespace)
    {
        if (namespace == null)
        {
            return null;
        }
        String[] segments = namespace.split("\\.", -1);
        for (int i = 0; i < segments.length; i++)
        {
            segments[i] = escape(segments[i]);
        }
        return String.join(".", segments);
    }
}
