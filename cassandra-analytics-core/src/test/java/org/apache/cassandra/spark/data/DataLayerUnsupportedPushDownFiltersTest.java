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
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestDataLayer;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

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
            assertNotNull(unsupportedFilters);
            assertEquals(0, unsupportedFilters.length);
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
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key
            assertEquals(0, unsupportedFilters.length);
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
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key
            assertEquals(0, unsupportedFilters.length);
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
            assertNotNull(unsupportedFilters);
            // In is supported and 'a' is the partition key
            assertEquals(0, unsupportedFilters.length);
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
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key, the clustering key 'b' is not pushed down
            assertEquals(1, unsupportedFilters.length);
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
            assertNotNull(unsupportedFilters);
            // EqualTo is supported and 'a' is the partition key, 'c' is not supported
            assertEquals(1, unsupportedFilters.length);
            assertSame(unsupportedNonPartitionKeyColumnFilter, unsupportedFilters[0]);
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
                assertNotNull(unsupportedFilters);
                // Not supported
                assertEquals(1, unsupportedFilters.length);
            }
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
            assertNotNull(unsupportedFilters);
            // Filter push-down is disabled because not all partition columns are in the filter array
            assertEquals(1, unsupportedFilters.length);

            // a and b are part of a composite partition column
            allFilters = new Filter[]{new EqualTo("a", 5), new EqualTo("b", 10)};
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // Filter push-down is disabled because not all partition columns are in the filter array
            assertEquals(2, unsupportedFilters.length);

            // a and b are part of a composite partition column, but d is not
            allFilters = new Filter[]{new EqualTo("a", 5), new EqualTo("b", 10), new EqualTo("d", 20)};
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // Filter push-down is disabled because not all partition columns are in the filter array
            assertEquals(3, unsupportedFilters.length);

            // a and b are part of a composite partition column
            allFilters = new Filter[]{new EqualTo("a", 5), new EqualTo("b", 10), new EqualTo("c", 15)};
            unsupportedFilters = dataLayer.unsupportedPushDownFilters(allFilters);
            assertNotNull(unsupportedFilters);
            // Filter push-down is enabled because all the partition columns are part of the filter array
            assertEquals(0, unsupportedFilters.length);
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
            assertNotNull(unsupportedFilters);
            // Filter push-down is disabled because the partition column is missing in the filters
            assertEquals(1, unsupportedFilters.length);
        });
    }

    private TestSchema schemaWithCompositePartitionKey(CassandraBridge bridge)
    {
        return TestSchema.builder()
                         .withPartitionKey("a", bridge.aInt())
                         .withPartitionKey("b", bridge.aInt())
                         .withPartitionKey("c", bridge.aInt())
                         .withClusteringKey("d", bridge.aInt())
                         .withColumn("e", bridge.aInt()).build();
    }
}
