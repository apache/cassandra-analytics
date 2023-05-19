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

import org.junit.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class CqlFieldTests extends VersionRunner
{
    public CqlFieldTests(CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testEquality()
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        assertNotSame(field1, field2);
        assertEquals(field1, field2);
        assertEquals(field1.hashCode(), field2.hashCode());
        assertNotEquals(null, field1);
        assertNotEquals(null, field2);
        assertNotEquals(new ArrayList<>(), field1);
        assertEquals(field1, field1);
    }

    @Test
    public void testNotEqualsName()
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "b", bridge.bigint(), 0);
        assertNotSame(field1, field2);
        assertNotEquals(field1, field2);
        assertNotEquals(field1.hashCode(), field2.hashCode());
    }

    @Test
    public void testNotEqualsType()
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "a", bridge.timestamp(), 0);
        assertNotSame(field1, field2);
        assertNotEquals(field1, field2);
        assertNotEquals(field1.hashCode(), field2.hashCode());
    }

    @Test
    public void testNotEqualsKey()
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(false, true, false, "a", bridge.bigint(), 0);
        assertNotSame(field1, field2);
        assertNotEquals(field1, field2);
        assertNotEquals(field1.hashCode(), field2.hashCode());
    }

    @Test
    public void testNotEqualsPos()
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "a", bridge.bigint(), 1);
        assertNotSame(field1, field2);
        assertNotEquals(field1, field2);
        assertNotEquals(field1.hashCode(), field2.hashCode());
    }

    @Test
    public void testCqlTypeParser()
    {
        testCqlTypeParser("set<text>", bridge.text());
        testCqlTypeParser("set<float>", bridge.aFloat());
        testCqlTypeParser("set<time>", bridge.time());
        testCqlTypeParser("SET<BLOB>", bridge.blob());
        testCqlTypeParser("list<ascii>", bridge.ascii());
        testCqlTypeParser("list<int>", bridge.aInt());
        testCqlTypeParser("LIST<BIGINT>", bridge.bigint());
        testCqlTypeParser("map<int,text>", bridge.aInt(), bridge.text());
        testCqlTypeParser("map<boolean , decimal>", bridge.bool(), bridge.decimal());
        testCqlTypeParser("MAP<TIMEUUID,TIMESTAMP>", bridge.timeuuid(), bridge.timestamp());
        testCqlTypeParser("MAP<VARCHAR , double>", bridge.varchar(), bridge.aDouble());
        testCqlTypeParser("tuple<int, text>", bridge.aInt(), bridge.text());
    }

    @Test
    public void testSplitMapTypes()
    {
        splitMap("", "", null);
        splitMap("text", "text", null);
        splitMap("bigint", "bigint", null);
        splitMap("set<text>", "set<text>", null);
        splitMap("text,bigint", "text", "bigint");
        splitMap("varchar , float", "varchar", "float");
        splitMap("varchar , float", "varchar", "float");
        splitMap("date, frozen<set<text>>", "date", "frozen<set<text>>");
        splitMap("timestamp, frozen<map<int, blob>>", "timestamp", "frozen<map<int, blob>>");
        splitMap("frozen<list<timeuuid>>, frozen<map<uuid, double>>",
                 "frozen<list<timeuuid>>", "frozen<map<uuid, double>>");
        splitMap("frozen<map<int, float>>, frozen<map<blob, decimal>>",
                 "frozen<map<int, float>>", "frozen<map<blob, decimal>>");
        splitMap("frozen<map<int,float>>,frozen<map<blob,decimal>>",
                 "frozen<map<int,float>>", "frozen<map<blob,decimal>>");
        splitMap("text, frozen<map<text, set<text>>>", "text", "frozen<map<text, set<text>>>");
        splitMap("frozen<map<set<int>,blob>>,   frozen<map<text, frozen<map<bigint, double>>>>",
                 "frozen<map<set<int>,blob>>", "frozen<map<text, frozen<map<bigint, double>>>>");
    }

    @Test
    public void testCqlNames()
    {
        assertEquals("set<bigint>", bridge.collection("set", bridge.bigint()).cqlName());
        assertEquals("list<timestamp>", bridge.collection("LIST", bridge.timestamp()).cqlName());
        assertEquals("map<text, int>", bridge.collection("Map", bridge.text(), bridge.aInt()).cqlName());
        assertEquals("tuple<int, blob, varchar>",
                     bridge.collection("tuple", bridge.aInt(), bridge.blob(), bridge.varchar()).cqlName());
        assertEquals("tuple<int, blob, map<int, float>>",
                     bridge.collection("tuPLe", bridge.aInt(), bridge.blob(), bridge.map(bridge.aInt(), bridge.aFloat())).cqlName());
    }

    @Test
    public void testTuple()
    {
        String[] result = CassandraBridge.splitInnerTypes("a, b, c, d,e, f, g");
        assertEquals("a", result[0]);
        assertEquals("b", result[1]);
        assertEquals("c", result[2]);
        assertEquals("d", result[3]);
        assertEquals("e", result[4]);
        assertEquals("f", result[5]);
        assertEquals("g", result[6]);
    }

    private static void splitMap(String str, String left, String right)
    {
        String[] result = CassandraBridge.splitInnerTypes(str);
        if (left != null)
        {
            assertEquals(left, result[0]);
        }
        if (right != null)
        {
            assertEquals(right, result[1]);
        }
    }

    @Test
    public void testNestedSet()
    {
        CqlField.CqlType type = bridge.parseType("set<frozen<map<text, list<double>>>>");
        assertNotNull(type);
        assertEquals(type.internalType(), CqlField.CqlType.InternalType.Set);
        CqlField.CqlType frozen = ((CqlField.CqlSet) type).type();
        assertEquals(frozen.internalType(), CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlMap map = (CqlField.CqlMap) ((CqlField.CqlFrozen) frozen).inner();
        assertEquals(map.keyType(), bridge.text());
        assertEquals(map.valueType().internalType(), CqlField.CqlType.InternalType.List);
        CqlField.CqlList list = (CqlField.CqlList) map.valueType();
        assertEquals(list.type(), bridge.aDouble());
    }

    @Test
    public void testFrozenCqlTypeParser()
    {
        CqlField.CqlType type = bridge.parseType("frozen<map<text, float>>");
        assertNotNull(type);
        assertEquals(type.internalType(), CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlType inner = ((CqlField.CqlFrozen) type).inner();
        assertEquals(inner.internalType(), CqlField.CqlType.InternalType.Map);
        CqlField.CqlMap map = (CqlField.CqlMap) inner;
        assertEquals(map.keyType(), bridge.text());
        assertEquals(map.valueType(), bridge.aFloat());
    }

    @Test
    public void testFrozenCqlTypeNested()
    {
        CqlField.CqlType type = bridge.parseType("map<frozen<set<text>>, frozen<map<int, list<blob>>>>");
        assertNotNull(type);
        assertEquals(type.internalType(), CqlField.CqlType.InternalType.Map);

        CqlField.CqlType key = ((CqlField.CqlMap) type).keyType();
        assertEquals(key.internalType(), CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlCollection keyInner = (CqlField.CqlCollection) ((CqlField.CqlFrozen) key).inner();
        assertEquals(keyInner.internalType(), CqlField.CqlType.InternalType.Set);
        assertEquals(keyInner.type(), bridge.text());

        CqlField.CqlType value = ((CqlField.CqlMap) type).valueType();
        assertEquals(value.internalType(), CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlCollection valueInner = (CqlField.CqlCollection) ((CqlField.CqlFrozen) value).inner();
        assertEquals(valueInner.internalType(), CqlField.CqlType.InternalType.Map);
        CqlField.CqlMap valueMap = (CqlField.CqlMap) valueInner;
        assertEquals(valueMap.keyType(), bridge.aInt());
        assertEquals(valueMap.valueType().internalType(), CqlField.CqlType.InternalType.List);
        assertEquals(((CqlField.CqlList) valueMap.valueType()).type(), bridge.blob());
    }

    private void testCqlTypeParser(String str, CqlField.CqlType expectedType)
    {
        testCqlTypeParser(str, expectedType, null);
    }

    private void testCqlTypeParser(String str, CqlField.CqlType expectedType, CqlField.CqlType otherType)
    {
        CqlField.CqlType type = bridge.parseType(str);
        if (type instanceof CqlField.CqlTuple)
        {
            assertEquals(((CqlField.CqlTuple) type).type(0), expectedType);
            if (otherType != null)
            {
                assertEquals(((CqlField.CqlTuple) type).type(1), otherType);
            }
        }
        else if (type instanceof CqlField.CqlCollection)
        {
            assertEquals(((CqlField.CqlCollection) type).type(), expectedType);
            if (otherType != null)
            {
                assertTrue(type instanceof CqlField.CqlMap);
                assertEquals(((CqlField.CqlMap) type).valueType(), otherType);
            }
        }
        else
        {
            assertTrue(type instanceof CqlField.NativeType);
            assertEquals(type, expectedType);
        }
    }
}
