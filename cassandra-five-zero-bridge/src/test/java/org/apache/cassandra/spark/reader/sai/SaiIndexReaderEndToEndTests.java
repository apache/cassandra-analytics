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
package org.apache.cassandra.spark.reader.sai;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.FileSystemSSTable;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.BufferingInputStreamStats;
import org.apache.cassandra.utils.TokenUtils;

import static org.apache.cassandra.spark.TestUtils.BIG_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SaiIndexReaderEndToEndTests
{
    static
    {
        CassandraBridgeImplementation.setup();
    }

    @Test
    void testFindCandidateTokensWithRealSSTable(@TempDir Path directory) throws Exception
    {
        String keyspace = "sai_reader_" + UUID.randomUUID().toString().replace("-", "");
        String table = keyspace + ".indexed_rows";
        String indexName = "value_idx";

        String createTable = "CREATE TABLE " + table + " (id int PRIMARY KEY, value int)";
        String insert = "INSERT INTO " + table + " (id, value) VALUES (?, ?)";
        String createIndex = "CREATE INDEX " + indexName + " ON " + table + " (value) USING 'sai'";

        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                       .inDirectory(directory.toString())
                                                       .forTable(createTable)
                                                       .using(insert)
                                                       .withIndexes(createIndex)
                                                       .withFormat(BIG_FORMAT)
                                                       .withPartitioner(Murmur3Partitioner.instance)
                                                       .build())
        {
            writer.addRow(1, 7);
            writer.addRow(2, 9); // indexed, but does not satisfy the filter
            writer.addRow(3, 7);
            writer.addRow(4, 7);
            writer.addRow(5, 9); // indexed, but does not satisfy the filter
        }

        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, "indexed_rows");
        assertThat(metadata).isNotNull();
        assertThat(Keyspace.openWithoutSSTables(keyspace)).isNotNull();
        assertThat(Keyspace.openWithoutSSTables(keyspace)
                           .getColumnFamilyStore("indexed_rows")).isNotNull();

        SaiFilter filter = new SaiFilter(new SaiIndex(indexName, "value", false, Collections.emptyMap()),
                                         SaiFilter.Operator.EQ,
                                         Int32Type.instance.decompose(7).array());

        Set<SSTable> sstables;
        try (Stream<Path> paths = Files.list(directory))
        {
            sstables = paths.filter(path -> path.getFileName().toString().endsWith("-Data.db"))
                            .map(path -> (SSTable) new FileSystemSSTable(path, false, () -> BufferingInputStreamStats.doNothingStats()))
                            .collect(Collectors.toSet());
        }
        assertThat(sstables).hasSize(1);
        assertThat(sstables.iterator().next().saiComponentNames()).isNotEmpty();

        List<BigInteger> matchingTokens = new ArrayList<>();
        for (int id : Arrays.asList(1, 3, 4))
        {
            Token token = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(id)).getToken();
            matchingTokens.add(TokenUtils.tokenToBigInteger(token));
        }
        Collections.sort(matchingTokens);

        // No Spark range: verify real index hits, not an error-driven fallback.
        CandidateTokens all = SaiIndexReader.findCandidateTokens(metadata, sstables,
                                                                 Collections.singletonList(filter), null, 100)
                                            .orElseThrow();
        assertThat(all.size()).isEqualTo(3);
        for (BigInteger token : matchingTokens)
        {
            assertThat(all.contains(token)).isTrue();
        }

        // Seeking must skip the first matching partition (open lower bound),
        // while the other two matching partitions remain visible.
        SparkRangeFilter range = SparkRangeFilter.create(TokenRange.openClosed(matchingTokens.get(0),
                                                                               matchingTokens.get(2)));
        CandidateTokens restricted = SaiIndexReader.findCandidateTokens(metadata, sstables,
                                                                        Collections.singletonList(filter), range, 100)
                                                   .orElseThrow();
        assertThat(restricted.size()).isEqualTo(2);
        assertThat(restricted.contains(matchingTokens.get(0))).isFalse();
        assertThat(restricted.contains(matchingTokens.get(1))).isTrue();
        assertThat(restricted.contains(matchingTokens.get(2))).isTrue();

        // Test query producing more unique tokens than the limit fails open.
        assertThat(SaiIndexReader.findCandidateTokens(metadata, sstables,
                                                      Collections.singletonList(filter), null, 2)).isEmpty();

        // Test missing SAI component must also fail to open.
        SSTable withoutSai = mock(SSTable.class);
        when(withoutSai.saiComponentNames()).thenReturn(Collections.emptySet());
        when(withoutSai.getDataFileName()).thenReturn("oa-1-big-Data.db");
        assertThat(SaiIndexReader.findCandidateTokens(metadata, Collections.singleton(withoutSai),
                                                      Collections.singletonList(filter), null, 100)).isEmpty();
    }
}
