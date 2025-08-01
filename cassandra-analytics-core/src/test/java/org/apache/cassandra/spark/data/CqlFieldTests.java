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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;

import static org.assertj.core.api.Assertions.assertThat;

public class CqlFieldTests extends VersionRunner
{

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testEquality(CassandraBridge bridge)
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        assertThat(field1).isNotSameAs(field2);
        assertThat(field1).isEqualTo(field2);
        assertThat(field1.hashCode()).isEqualTo(field2.hashCode());
        assertThat(field1).isNotEqualTo(null);
        assertThat(field2).isNotEqualTo(null);
        assertThat(field1).isNotEqualTo(new ArrayList<>());
        assertThat(field1).isEqualTo(field1);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNotEqualsName(CassandraBridge bridge)
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "b", bridge.bigint(), 0);
        assertThat(field1).isNotSameAs(field2);
        assertThat(field1).isNotEqualTo(field2);
        assertThat(field1.hashCode()).isNotEqualTo(field2.hashCode());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNotEqualsType(CassandraBridge bridge)
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "a", bridge.timestamp(), 0);
        assertThat(field1).isNotSameAs(field2);
        assertThat(field1).isNotEqualTo(field2);
        assertThat(field1.hashCode()).isNotEqualTo(field2.hashCode());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNotEqualsKey(CassandraBridge bridge)
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(false, true, false, "a", bridge.bigint(), 0);
        assertThat(field1).isNotSameAs(field2);
        assertThat(field1).isNotEqualTo(field2);
        assertThat(field1.hashCode()).isNotEqualTo(field2.hashCode());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNotEqualsPos(CassandraBridge bridge)
    {
        CqlField field1 = new CqlField(true, false, false, "a", bridge.bigint(), 0);
        CqlField field2 = new CqlField(true, false, false, "a", bridge.bigint(), 1);
        assertThat(field1).isNotSameAs(field2);
        assertThat(field1).isNotEqualTo(field2);
        assertThat(field1.hashCode()).isNotEqualTo(field2.hashCode());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCqlTypeParser(CassandraBridge bridge)
    {
        testCqlTypeParser("set<text>", bridge.text(), bridge);
        testCqlTypeParser("set<float>", bridge.aFloat(), bridge);
        testCqlTypeParser("set<time>", bridge.time(), bridge);
        testCqlTypeParser("SET<BLOB>", bridge.blob(), bridge);
        testCqlTypeParser("list<ascii>", bridge.ascii(), bridge);
        testCqlTypeParser("list<int>", bridge.aInt(), bridge);
        testCqlTypeParser("LIST<BIGINT>", bridge.bigint(), bridge);
        testCqlTypeParser("map<int,text>", bridge.aInt(), bridge.text(), bridge);
        testCqlTypeParser("map<boolean , decimal>", bridge.bool(), bridge.decimal(), bridge);
        testCqlTypeParser("MAP<TIMEUUID,TIMESTAMP>", bridge.timeuuid(), bridge.timestamp(), bridge);
        testCqlTypeParser("MAP<VARCHAR , double>", bridge.varchar(), bridge.aDouble(), bridge);
        testCqlTypeParser("tuple<int, text>", bridge.aInt(), bridge.text(), bridge);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSplitMapTypes(CassandraBridge bridge)
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

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testCqlNames(CassandraBridge bridge)
    {
        assertThat(bridge.collection("set", bridge.bigint()).cqlName()).isEqualTo("set<bigint>");
        assertThat(bridge.collection("LIST", bridge.timestamp()).cqlName()).isEqualTo("list<timestamp>");
        assertThat(bridge.collection("Map", bridge.text(), bridge.aInt()).cqlName()).isEqualTo("map<text, int>");
        assertThat(bridge.collection("tuple", bridge.aInt(), bridge.blob(), bridge.varchar()).cqlName())
            .isEqualTo("tuple<int, blob, varchar>");
        assertThat(bridge.collection("tuPLe", bridge.aInt(), bridge.blob(), bridge.map(bridge.aInt(), bridge.aFloat())).cqlName())
            .isEqualTo("tuple<int, blob, map<int, float>>");
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testTuple(CassandraBridge bridge)
    {
        String[] result = CassandraTypes.splitInnerTypes("a, b, c, d,e, f, g");
        assertThat(result[0]).isEqualTo("a");
        assertThat(result[1]).isEqualTo("b");
        assertThat(result[2]).isEqualTo("c");
        assertThat(result[3]).isEqualTo("d");
        assertThat(result[4]).isEqualTo("e");
        assertThat(result[5]).isEqualTo("f");
        assertThat(result[6]).isEqualTo("g");
    }

    private static void splitMap(String str, String left, String right)
    {
        String[] result = CassandraTypes.splitInnerTypes(str);
        if (left != null)
        {
            assertThat(result[0]).isEqualTo(left);
        }
        if (right != null)
        {
            assertThat(result[1]).isEqualTo(right);
        }
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testNestedSet(CassandraBridge bridge)
    {
        CqlField.CqlType type = bridge.parseType("set<frozen<map<text, list<double>>>>");
        assertThat(type).isNotNull();
        assertThat(type.internalType()).isEqualTo(CqlField.CqlType.InternalType.Set);
        CqlField.CqlType frozen = ((CqlField.CqlSet) type).type();
        assertThat(frozen.internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlMap map = (CqlField.CqlMap) ((CqlField.CqlFrozen) frozen).inner();
        assertThat(map.keyType()).isEqualTo(bridge.text());
        assertThat(map.valueType().internalType()).isEqualTo(CqlField.CqlType.InternalType.List);
        CqlField.CqlList list = (CqlField.CqlList) map.valueType();
        assertThat(list.type()).isEqualTo(bridge.aDouble());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testFrozenCqlTypeParser(CassandraBridge bridge)
    {
        CqlField.CqlType type = bridge.parseType("frozen<map<text, float>>");
        assertThat(type).isNotNull();
        assertThat(type.internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlType inner = ((CqlField.CqlFrozen) type).inner();
        assertThat(inner.internalType()).isEqualTo(CqlField.CqlType.InternalType.Map);
        CqlField.CqlMap map = (CqlField.CqlMap) inner;
        assertThat(map.keyType()).isEqualTo(bridge.text());
        assertThat(map.valueType()).isEqualTo(bridge.aFloat());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testFrozenCqlTypeNested(CassandraBridge bridge)
    {
        CqlField.CqlType type = bridge.parseType("map<frozen<set<text>>, frozen<map<int, list<blob>>>>");
        assertThat(type).isNotNull();
        assertThat(type.internalType()).isEqualTo(CqlField.CqlType.InternalType.Map);

        CqlField.CqlType key = ((CqlField.CqlMap) type).keyType();
        assertThat(key.internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlCollection keyInner = (CqlField.CqlCollection) ((CqlField.CqlFrozen) key).inner();
        assertThat(keyInner.internalType()).isEqualTo(CqlField.CqlType.InternalType.Set);
        assertThat(keyInner.type()).isEqualTo(bridge.text());

        CqlField.CqlType value = ((CqlField.CqlMap) type).valueType();
        assertThat(value.internalType()).isEqualTo(CqlField.CqlType.InternalType.Frozen);
        CqlField.CqlCollection valueInner = (CqlField.CqlCollection) ((CqlField.CqlFrozen) value).inner();
        assertThat(valueInner.internalType()).isEqualTo(CqlField.CqlType.InternalType.Map);
        CqlField.CqlMap valueMap = (CqlField.CqlMap) valueInner;
        assertThat(valueMap.keyType()).isEqualTo(bridge.aInt());
        assertThat(valueMap.valueType().internalType()).isEqualTo(CqlField.CqlType.InternalType.List);
        assertThat(((CqlField.CqlList) valueMap.valueType()).type()).isEqualTo(bridge.blob());
    }

    private void testCqlTypeParser(String str, CqlField.CqlType expectedType, CassandraBridge bridge)
    {
        testCqlTypeParser(str, expectedType, null, bridge);
    }

    private void testCqlTypeParser(String str, CqlField.CqlType expectedType, CqlField.CqlType otherType, CassandraBridge bridge)
    {
        CqlField.CqlType type = bridge.parseType(str);
        if (type instanceof CqlField.CqlTuple)
        {
            assertThat(((CqlField.CqlTuple) type).type(0)).isEqualTo(expectedType);
            if (otherType != null)
            {
                assertThat(((CqlField.CqlTuple) type).type(1)).isEqualTo(otherType);
            }
        }
        else if (type instanceof CqlField.CqlCollection)
        {
            assertThat(((CqlField.CqlCollection) type).type()).isEqualTo(expectedType);
            if (otherType != null)
            {
                assertThat(type).isInstanceOf(CqlField.CqlMap.class);
                assertThat(((CqlField.CqlMap) type).valueType()).isEqualTo(otherType);
            }
        }
        else
        {
            assertThat(type).isInstanceOf(CqlField.NativeType.class);
            assertThat(type).isEqualTo(expectedType);
        }
    }
}
