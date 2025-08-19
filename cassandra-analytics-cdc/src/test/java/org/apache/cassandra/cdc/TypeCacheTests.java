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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TypeCacheTests
{
    @Test
    public void testTypeCache()
    {
        TypeCache typeCache = TypeCache.get(CassandraVersion.FOURZERO);
        assertThat(typeCache.cqlTypeCache).isNull();

        CqlField.CqlType ksBigInt = typeCache.getType("ks", "bigint");
        assertThat(typeCache.cqlTypeCache).isNotNull();
        assertThat(ksBigInt.cqlName()).isEqualTo("bigint");
        assertThat(typeCache.cqlTypeCache.size()).isOne();
        typeCache.getType("ks", "bigint");
        typeCache.getType("ks", "bigint");
        typeCache.getType("ks", "bigint");
        assertThat(typeCache.cqlTypeCache.size()).isOne();

        CqlField.CqlType ks2BigInt = typeCache.getType("ks2", "bigint");
        assertThat(ks2BigInt.cqlName()).isEqualTo("bigint");
        assertThat(ks2BigInt).isSameAs(ksBigInt);

        assertThat(typeCache.getType("ks2", "int")).isSameAs(typeCache.getType("ks", "int"));
        assertThat(typeCache.getType("ks2", "text")).isSameAs(typeCache.getType("ks", "text"));
        assertThat(typeCache.getType("ks2", "boolean")).isSameAs(typeCache.getType("ks", "boolean"));
        assertThat(typeCache.getType("ks2", "double")).isSameAs(typeCache.getType("ks", "double"));
        assertThat(typeCache.getType("ks2", "float")).isSameAs(typeCache.getType("ks", "float"));
        assertThat(typeCache.cqlTypeCache.size()).isEqualTo(12);

        // test collections
        assertThat(typeCache.getType("ks", "map<text, int>")).isInstanceOf(CqlField.CqlMap.class);
        assertThat(typeCache.getType("ks", "list<ascii>")).isInstanceOf(CqlField.CqlList.class);
        assertThat(typeCache.getType("ks", "set<date>")).isInstanceOf(CqlField.CqlSet.class);
        assertThat(typeCache.getType("ks", "frozen<set<blob>>")).isInstanceOf(CqlField.CqlFrozen.class);
        assertThat(typeCache.cqlTypeCache.size()).isEqualTo(16);

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
        assertThat(udtType.cqlName()).isEqualTo("my_udt");
        assertThat(typeCache.cqlTypeCache.size()).isEqualTo(17);

        assertThatThrownBy(() -> typeCache.getType("ks2", "my_udt"))
                .isInstanceOf(RuntimeException.class);
    }
}
