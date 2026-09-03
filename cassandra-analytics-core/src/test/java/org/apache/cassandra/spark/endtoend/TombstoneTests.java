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

package org.apache.cassandra.spark.endtoend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.spark.CommonTestUtils.qtRandom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.characters;
import static org.quicktheories.generators.SourceDSL.integers;

@Tag("Sequential")
public class TombstoneTests
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testPartitionTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 10;
        qt().withExamples(20)
            .forAll(integers().between(0, numRows - 2), qtRandom())
            .checkAssert((deleteRangeStart, random) -> {
                assert 0 <= deleteRangeStart && deleteRangeStart < numRows;
                int deleteRangeEnd = deleteRangeStart + random.nextInt(numRows - deleteRangeStart - 1) + 1;
                assert deleteRangeStart < deleteRangeEnd && deleteRangeEnd < numRows;

                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a ="))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = deleteRangeStart; row < deleteRangeEnd; row++)
                          {
                              writer.write(row);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              int value = row.getInt(0);
                              assertThat(value).isBetween(0, numRows - 1);
                              assertThat(value < deleteRangeStart || value >= deleteRangeEnd).isTrue();
                              count++;
                          }
                          assertThat(count).isEqualTo((numRows - (deleteRangeEnd - deleteRangeStart)) * numColumns);
                      })
                      .run(bridge.getVersion());
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRowTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 10;
        qt().withExamples(20)
            .forAll(integers().between(0, numColumns - 1))
            .checkAssert(colNum ->
                         Tester.builder(TestSchema.basicBuilder(bridge)
                                                  .withDeleteFields("a =", "b ="))
                               .withVersions(TestUtils.tombstoneTestableVersions())
                               .dontWriteRandomData()
                               .withSSTableWriter(writer -> {
                                   for (int row = 0; row < numRows; row++)
                                   {
                                       for (int column = 0; column < numColumns; column++)
                                       {
                                           writer.write(row, column, column);
                                       }
                                   }
                               })
                               .withTombstoneWriter(writer -> {
                                   for (int row = 0; row < numRows; row++)
                                   {
                                       writer.write(row, colNum);
                                   }
                               })
                               .dontCheckNumSSTables()
                               .withCheck(dataset -> {
                                   int count = 0;
                                   for (Row row : dataset.collectAsList())
                                   {
                                       int value = row.getInt(0);
                                       assertThat(row.getInt(0)).isBetween(0, numRows - 1);
                                       assertThat(row.getInt(1)).isNotEqualTo(colNum);
                                       assertThat(row.get(2)).isEqualTo(row.get(1));
                                       count++;
                                   }
                                   assertThat(count).isEqualTo(numRows * (numColumns - 1));
                               })
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRangeTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 128;
        qt().withExamples(10)
            .forAll(integers().between(0, numColumns - 1), qtRandom())
            .checkAssert((startBound, random) -> {
                assertThat(startBound).isLessThan(numColumns);
                int endBound = startBound + random.nextInt(numColumns - startBound);
                assertThat(endBound).isBetween(startBound, numColumns);
                int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a =", "b >=", "b <"))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, startBound, endBound);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              // Verify row values exist within correct range with range tombstoned values removed
                              int value = row.getInt(1);
                              assertThat(row.getInt(2)).isEqualTo(value);
                              assertThat(value).isLessThanOrEqualTo(numColumns);
                              assertThat(value < startBound || value >= endBound).isTrue();
                              count++;
                          }
                          assertThat(count).isEqualTo(numRows * (numColumns - numTombstones));
                      })
                      .run(bridge.getVersion());
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRangeTombstoneString(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 128;
        qt().withExamples(10)
            .forAll(characters().ascii(), qtRandom())
            .checkAssert((startBound, random) -> {
                assertThat((int) startBound).isLessThanOrEqualTo(numColumns);
                char endBound = (char) (startBound + random.nextInt(numColumns - startBound));
                assertThat(endBound).isBetween(startBound, (char) numColumns);
                int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", bridge.aInt())
                                         .withClusteringKey("b", bridge.text())
                                         .withColumn("c", bridge.aInt())
                                         .withDeleteFields("a =", "b >=", "b <"))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  String value = String.valueOf((char) column);
                                  writer.write(row, value, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, startBound.toString(), Character.toString(endBound));
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              // Verify row values exist within correct range with range tombstoned values removed
                              int character = row.getString(1).charAt(0);
                              assertThat(character).isLessThanOrEqualTo(numColumns);
                              assertThat(character < startBound || character >= endBound).isTrue();
                              count++;
                          }
                          assertThat(count).isEqualTo(numRows * (numColumns - numTombstones));
                      })
                      .run(bridge.getVersion());
            });
    }
}
