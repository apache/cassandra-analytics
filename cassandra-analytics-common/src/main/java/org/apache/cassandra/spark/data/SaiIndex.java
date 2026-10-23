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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

/**
 * Serializable description of a Cassandra Storage-Attached-Index (SAI).
 *
 * This intentionally contains only version-neutral metadata. Cassandra-version
 * specific bridge code converts it to native index metadata ({@code StorageAttachedIndex}).
 */
public final class SaiIndex implements Serializable
{
    private static final long serialVersionUID = 1L;

    @NotNull
    private final String name;
    @NotNull
    private final String column;
    private final boolean columnQuoted;
    @NotNull
    private final Map<String, String> options;

    public SaiIndex(@NotNull String name,
                    @NotNull String column,
                    boolean columnQuoted,
                    @NotNull Map<String, String> options)
    {
        this.name = name;
        this.column = column;
        this.columnQuoted = columnQuoted;
        this.options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
    }

    @NotNull
    public String name()
    {
        return name;
    }

    @NotNull
    public String column()
    {
        return column;
    }

    public boolean columnQuoted()
    {
        return columnQuoted;
    }

    @NotNull
    public String target()
    {
        return columnQuoted
               ? "\"" + column.replace("\"", "\"\"") + "\""
               : column;
    }

    @NotNull
    public Map<String, String> options()
    {
        return options;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (!(other instanceof SaiIndex))
        {
            return false;
        }
        SaiIndex that = (SaiIndex) other;
        return Objects.equals(name, that.name)
               && Objects.equals(column, that.column)
               && Objects.equals(columnQuoted, that.columnQuoted)
               && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, column, columnQuoted, options);
    }

    @Override
    public String toString()
    {
        return "SaiIndex{name=" + name + ", column=" + column + ", columnQuoted=" + columnQuoted + ", options=" + options + "}";
    }
}
