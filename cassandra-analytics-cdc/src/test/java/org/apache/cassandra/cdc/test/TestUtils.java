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

package org.apache.cassandra.cdc.test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;

public class TestUtils
{
    private TestUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static Object collectionDeleteMutation(CassandraVersion version, ByteBuffer key)
    {
        return CdcBridgeFactory.executeActionOnBridgeClassLoader(version, (classLoader) -> {
            Class<?> cellPathClass = Class.forName("org.apache.cassandra.db.rows.CellPath", true, classLoader);
            Method cellPathFactory = cellPathClass.getMethod("create", ByteBuffer.class);
            Object cellPath = cellPathFactory.invoke(null, key);

            Class<?> collectionElementClass = Class.forName("org.apache.cassandra.bridge.CollectionElement", true, classLoader);
            Method collectionElementFactory = collectionElementClass.getMethod("deleted", cellPathClass);
            return collectionElementFactory.invoke(null, cellPath);
        });
    }
}
