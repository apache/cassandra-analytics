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

package org.apache.cassandra.cdc;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TypeCacheTests
{
    @Test
    public void testTypeCache()
    {
        TypeCache typeCache = TypeCache.get(CassandraVersion.FOURZERO);
        assertNull(typeCache.cqlTypeCache);

        CqlField.CqlType ksBigInt = typeCache.getType("ks", "bigint");
        assertNotNull(typeCache.cqlTypeCache);
        assertEquals("bigint", ksBigInt.cqlName());
        assertEquals(1, typeCache.cqlTypeCache.size());
        typeCache.getType("ks", "bigint");
        typeCache.getType("ks", "bigint");
        typeCache.getType("ks", "bigint");
        assertEquals(1, typeCache.cqlTypeCache.size());

        CqlField.CqlType ks2BigInt = typeCache.getType("ks2", "bigint");
        assertEquals("bigint", ks2BigInt.cqlName());
        assertSame(ksBigInt, ks2BigInt);

        assertSame(typeCache.getType("ks", "int"), typeCache.getType("ks2", "int"));
        assertSame(typeCache.getType("ks", "text"), typeCache.getType("ks2", "text"));
        assertSame(typeCache.getType("ks", "boolean"), typeCache.getType("ks2", "boolean"));
        assertSame(typeCache.getType("ks", "double"), typeCache.getType("ks2", "double"));
        assertSame(typeCache.getType("ks", "float"), typeCache.getType("ks2", "float"));
        assertEquals(12, typeCache.cqlTypeCache.size());

        // test collections
        assertInstanceOf(CqlField.CqlMap.class, typeCache.getType("ks", "map<text, int>"));
        assertInstanceOf(CqlField.CqlList.class, typeCache.getType("ks", "list<ascii>"));
        assertInstanceOf(CqlField.CqlSet.class, typeCache.getType("ks", "set<date>"));
        assertInstanceOf(CqlField.CqlFrozen.class, typeCache.getType("ks", "frozen<set<blob>>"));
        assertEquals(16, typeCache.cqlTypeCache.size());

        // test udt
        CassandraTypes cassandraTypes = typeCache.getTypes();
        cassandraTypes.updateUDTs("ks", cassandraTypes
                                        .udt("ks", "my_udt")
                                        .withField("a", cassandraTypes.bigint())
                                        .withField("b", cassandraTypes.text())
                                        .withField("c", cassandraTypes.inet())
                                        .withField("d", cassandraTypes.aInt())
                                        .build());
        CqlField.CqlType udtType = typeCache.getType("ks", "my_udt");
        assertEquals("my_udt", udtType.cqlName());
        assertEquals(17, typeCache.cqlTypeCache.size());

        assertThrows(RuntimeException.class, () -> assertNull(typeCache.getType("ks2", "my_udt")));
    }
}
