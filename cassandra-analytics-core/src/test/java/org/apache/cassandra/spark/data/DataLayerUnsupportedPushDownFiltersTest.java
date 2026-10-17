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

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;

import static org.apache.cassandra.spark.TestUtils.getFileType;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.assertj.core.api.Assertions.assertThat;

public class DataLayerUnsupportedPushDownFiltersTest
{
    @Test
    public void testNoFilters()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(new Filter[0]);
            assertThat(unsupportedFilters).isNotNull();
            assertThat(unsupportedFilters).hasSize(0);
        });
    }

    @Test
    public void testSupportedEqualToFilter()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            Filter[] allFilters = {new EqualTo("a", 5)};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // EqualTo is supported and 'a' is the partition key
            assertThat(unsupportedFilters).hasSize(0);
        });
    }

    @Test
    public void testSupportedFilterCaseInsensitive()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            Filter[] allFilters = {new EqualTo("A", 5)};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // EqualTo is supported and 'a' is the partition key
            assertThat(unsupportedFilters).hasSize(0);
        });
    }

    @Test
    public void testSupportedInFilter()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            Filter[] allFilters = {new In("a", new Object[]{5, 6, 7})};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // In is supported and 'a' is the partition key
            assertThat(unsupportedFilters).hasSize(0);
        });
    }

    @Test
    public void testSupportedEqualFilterWithClusteringKey()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            Filter[] allFilters = {new EqualTo("a", 5), new EqualTo("b", 8)};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // EqualTo is supported and 'a' is the partition key, the clustering key 'b' is not pushed down
            assertThat(unsupportedFilters).hasSize(1);
        });
    }

    @Test
    public void testUnsupportedEqualFilterWithColumn()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            EqualTo unsupportedNonPartitionKeyColumnFilter = new EqualTo("c", 25);
            Filter[] allFilters = {new EqualTo("a", 5), unsupportedNonPartitionKeyColumnFilter};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // EqualTo is supported and 'a' is the partition key, 'c' is not supported
            assertThat(unsupportedFilters).hasSize(1);
            assertThat(unsupportedFilters[0]).isSameAs(unsupportedNonPartitionKeyColumnFilter);
        });
    }

    @Test
    public void testUnsupportedFilters()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            List<Filter> unsupportedFilterList = ImmutableList.of(new EqualNullSafe("a", 5),
                                                                  new GreaterThan("a", 5),
                                                                  new GreaterThanOrEqual("a", 5),
                                                                  new LessThan("a", 5),
                                                                  new LessThanOrEqual("a", 5),
                                                                  new IsNull("a"),
                                                                  new IsNotNull("a"),
                                                                  new And(new EqualTo("a", 5), new EqualTo("b", 6)),
                                                                  new Or(new EqualTo("a", 5), new EqualTo("b", 6)),
                                                                  new Not(new In("a", new Object[]{5, 6, 7})),
                                                                  new StringStartsWith("a", "abc"),
                                                                  new StringEndsWith("a", "abc"),
                                                                  new StringContains("a", "abc"));

            for (Filter unsupportedFilter : unsupportedFilterList)
            {
                Filter[] allFilters = {unsupportedFilter};
                Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
                assertThat(unsupportedFilters).isNotNull();
                // Not supported
                assertThat(unsupportedFilters).hasSize(1);
            }
        });
    }

    @Test
    public void testSaiFiltersArePruningHintsOnly()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable())
            {
                @Override
                public List<SaiIndex> saiIndexes()
                {
                    return ImmutableList.of(new SaiIndex("c_idx", "c", false, Collections.emptyMap()),
                                            new SaiIndex("b_idx", "b", false, Collections.emptyMap()));
                }
            };

            EqualTo equality = new EqualTo("c", 25);
            GreaterThan lowerBound = new GreaterThan("C", 10);
            LessThan otherColumn = new LessThan("b", 100);
            Filter[] allFilters = {equality, lowerBound, otherColumn};

            List<SaiFilter> saiFilters = dataLayer.saiFilters(allFilters);
            assertThat(saiFilters).hasSize(3);
            assertThat(saiFilters.get(0).operator()).isEqualTo(SaiFilter.Operator.EQ);
            assertThat(saiFilters.get(0).value()).isEqualTo("25");
            assertThat(saiFilters.get(1).operator()).isEqualTo(SaiFilter.Operator.GT);
            assertThat(saiFilters.get(1).value()).isEqualTo("10");
            assertThat(saiFilters.get(2).index().name()).isEqualTo("b_idx");
            assertThat(saiFilters.get(2).operator()).isEqualTo(SaiFilter.Operator.LT);
            assertThat(saiFilters.get(2).value()).isEqualTo("100");

            // SAI is only an I/O pruning hint. Spark must still evaluate the predicates for correctness.
            assertThat(dataLayer.unsupportedPushDownFilters(allFilters)).containsExactly(allFilters);

            List<SaiFilter> nested = dataLayer.saiFilters(new Filter[]{new And(equality, otherColumn)});
            assertThat(nested).hasSize(2);
            assertThat(nested.get(0).index().name()).isEqualTo("c_idx");
            assertThat(nested.get(1).index().name()).isEqualTo("b_idx");

            // Cassandra 5.0 SAI does not support OR, so even a fully indexed OR is not used for pruning.
            assertThat(dataLayer.saiFilters(new Filter[]{new Or(equality, otherColumn)})).isEmpty();

            // An unsupported OR subtree does not prevent an independent AND conjunct from being used safely.
            List<SaiFilter> nestedBoolean = dataLayer.saiFilters(new Filter[]{new And(new Or(equality, otherColumn),
                                                                                      lowerBound)});
            assertThat(nestedBoolean).containsExactly(new SaiFilter(new SaiIndex("c_idx", "c", false, Collections.emptyMap()),
                                                                    SaiFilter.Operator.GT,
                                                                    "10"));

            List<SaiFilter> partialOr = dataLayer.saiFilters(new Filter[]{new Or(equality,
                                                                                 new StringContains("not_indexed", "x"))});
            assertThat(partialOr).isEmpty();

            // A partial AND may still use the SAI-capable branch as a conservative pruning hint.
            List<SaiFilter> partialAnd = dataLayer.saiFilters(new Filter[]{new And(equality,
                                                                                   new StringContains("not_indexed", "x"))});
            assertThat(partialAnd).containsExactly(new SaiFilter(new SaiIndex("c_idx", "c", false, Collections.emptyMap()),
                                                                 SaiFilter.Operator.EQ,
                                                                 "25"));
        });
    }

    @Test
    public void testSaiFilterColumnMatchingCaseSensitiveIdentifiers()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.builder(bridge)
                                          .withPartitionKey("id", bridge.aInt())
                                          .withColumn("foo", bridge.aInt())
                                          .withColumn("Foo", bridge.aInt())
                                .build();
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable())
            {
                @Override
                public List<SaiIndex> saiIndexes()
                {
                    return ImmutableList.of(new SaiIndex("upper_idx", "Foo", true, Collections.emptyMap()),
                                            new SaiIndex("lower_idx", "foo", false, Collections.emptyMap()));
                }
            };

            List<SaiFilter> exactUpper = dataLayer.saiFilters(new Filter[]{new EqualTo("Foo", 1)});
            assertThat(exactUpper).hasSize(1);
            assertThat(exactUpper.get(0).index().name()).isEqualTo("upper_idx");

            List<SaiFilter> exactLower = dataLayer.saiFilters(new Filter[]{new EqualTo("foo", 1)});
            assertThat(exactLower).hasSize(1);
            assertThat(exactLower.get(0).index().name()).isEqualTo("lower_idx");

            // Without an exact spelling, case-insensitive lookup is ambiguous. Do not prune with either index.
            assertThat(dataLayer.saiFilters(new Filter[]{new EqualTo("FOO", 1)})).isEmpty();
        });
    }

    @Test
    public void testSchemaWithCompositePartitionKey()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = schemaWithCompositePartitionKey(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            // a is part of a composite partition column
            Filter[] allFilters = {new EqualTo("a", 5)};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // Filter push-down is disabled because not all partition columns are in the filter array
            assertThat(unsupportedFilters).hasSize(1);

            // a and b are part of a composite partition column
            allFilters = new Filter[]{new EqualTo("a", 5), new EqualTo("b", 10)};
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // Filter push-down is disabled because not all partition columns are in the filter array
            assertThat(unsupportedFilters).hasSize(2);

            // a and b are part of a composite partition column, but d is not
            allFilters = new Filter[]{new EqualTo("a", 5), new EqualTo("b", 10), new EqualTo("d", 20)};
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // Filter push-down is disabled because not all partition columns are in the filter array
            assertThat(unsupportedFilters).hasSize(3);

            // a and b are part of a composite partition column
            allFilters = new Filter[]{new EqualTo("a", 5), new EqualTo("b", 10), new EqualTo("c", 15)};
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // Filter push-down is enabled because all the partition columns are part of the filter array
            assertThat(unsupportedFilters).isEmpty();
        });
    }

    @Test
    public void testDisablePushDownWhenPartitionKeyIsMissing()
    {
        runTest((partitioner, directory, bridge) -> {
            TestSchema schema = TestSchema.basic(bridge);
            List<Path> dataFiles = getFileType(directory, FileType.DATA).collect(Collectors.toList());
            TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles, schema.buildTable());

            // b is not the partition column
            Filter[] allFilters = {new EqualTo("b", 25)};
            Filter[] unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertThat(unsupportedFilters).isNotNull();
            // Filter push-down is disabled because the partition column is missing in the filters
            assertThat(unsupportedFilters).hasSize(1);
        });
    }

    private TestSchema schemaWithCompositePartitionKey(CassandraBridge bridge)
    {
        return TestSchema.builder(bridge)
                         .withPartitionKey("a", bridge.aInt())
                         .withPartitionKey("b", bridge.aInt())
                         .withPartitionKey("c", bridge.aInt())
                         .withClusteringKey("d", bridge.aInt())
                         .withColumn("e", bridge.aInt()).build();
    }
}
