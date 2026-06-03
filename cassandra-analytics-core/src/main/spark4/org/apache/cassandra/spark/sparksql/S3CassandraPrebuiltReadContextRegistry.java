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

package org.apache.cassandra.spark.sparksql;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.Nullable;

public final class S3CassandraPrebuiltReadContextRegistry
{
    private static final Map<String, S3CassandraPrebuiltReadContext> CONTEXTS = new ConcurrentHashMap<>();

    private S3CassandraPrebuiltReadContextRegistry()
    {
    }

    static S3CassandraPrebuiltReadContext register(S3CassandraPrebuiltReadContext context)
    {
        CONTEXTS.put(context.id(), context);
        return context;
    }

    @Nullable
    public static S3CassandraPrebuiltReadContext get(String id)
    {
        return CONTEXTS.get(id);
    }

    static void remove(String id)
    {
        CONTEXTS.remove(id);
    }

    static void clear()
    {
        CONTEXTS.clear();
    }
}
