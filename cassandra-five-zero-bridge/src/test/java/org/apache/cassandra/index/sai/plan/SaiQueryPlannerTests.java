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
package org.apache.cassandra.index.sai.plan;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

class SaiQueryPlannerTests
{
    @Test
    void testPlanCreation()
    {
        String keyspace = "sai_query_planner_tests";
        String table = "table1";

        new CassandraBridgeImplementation().
        buildSchema("CREATE TABLE table1 (id int PRIMARY KEY, score int)", keyspace);

        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        assertThat(metadata).isNotNull();

        SaiIndex index = new SaiIndex("score_idx", "score", false, Collections.emptyMap());
        SaiQueryPlanner.Plan plan = SaiQueryPlanner.plan(metadata,
                                                         Arrays.asList(new SaiFilter(index, SaiFilter.Operator.GTE,
                                                                                     ByteBufferUtil.getArray(Int32Type.instance.decompose(10))),
                                                                       new SaiFilter(index, SaiFilter.Operator.LTE,
                                                                                     ByteBufferUtil.getArray(Int32Type.instance.decompose(20)))));

        assertThat(plan).isNotNull();
        assertThat(plan.rowFilter().getExpressions()).hasSize(2);
        ColumnMetadata score = metadata.getColumn(ByteBufferUtil.bytes("score"));
        assertThat(score).isNotNull();

        RowFilter.Expression lower = plan.rowFilter().getExpressions().get(0);
        assertThat(lower.column()).isEqualTo(score);
        assertThat(lower.operator()).isEqualTo(Operator.GTE);
        assertThat(Int32Type.instance.compose(lower.getIndexValue())).isEqualTo(10);

        RowFilter.Expression upper = plan.rowFilter().getExpressions().get(1);
        assertThat(upper.column()).isEqualTo(score);
        assertThat(upper.operator()).isEqualTo(Operator.LTE);
        assertThat(Int32Type.instance.compose(upper.getIndexValue())).isEqualTo(20);

        // both bounds use the same native index (cached)
        assertThat(plan.resourceIndexes()).hasSize(1);
        assertThat(plan.indexFor(score)).isSameAs(plan.resourceIndexes().get(0));
        assertThat(plan.indexFor(score).getIndexMetadata().name).isEqualTo("score_idx");
    }
}
