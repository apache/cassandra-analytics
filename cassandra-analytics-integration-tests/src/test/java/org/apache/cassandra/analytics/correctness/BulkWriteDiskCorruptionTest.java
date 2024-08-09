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

package org.apache.cassandra.analytics.correctness;

import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import net.jpountz.lz4.LZ4Exception;
import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.BulkWriterContext;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BulkWriteDiskCorruptionTest extends SharedClusterSparkIntegrationTestBase
{
    private static final QualifiedName QUALIFIED_NAME = new QualifiedName(TEST_KEYSPACE, "test_write_disk_corruption");

    static
    {
        // Intercepts SortedSSTableWriter#validateSSTables to corrupt file on purpose.
        // Install the class rebase the earliest, before JVM loads the class
        BBHelperInterceptSortedSSTableWriterValidateSSTables.install();
    }

    @Test
    void testDiskCorruption()
    {
        Map<String, String> writerOptions = new HashMap<>();

        SparkSession spark = getOrCreateSparkSession();

        // Generate some data
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        // Write the data using Bulk Writer
        try
        {
            bulkWriterDataFrameWriter(dfWrite, QUALIFIED_NAME, writerOptions).save();
            fail("Bulk write should fail");
        }
        catch (Exception ex)
        {
            assertThat(ex)
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessageContaining("Bulk Write to Cassandra has failed")
            .rootCause()
            .isExactlyInstanceOf(LZ4Exception.class)
            .hasMessageContaining("Malformed input");
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(QUALIFIED_NAME, CREATE_TEST_TABLE_STATEMENT);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(1);
    }

    public static class BBHelperInterceptSortedSSTableWriterValidateSSTables
    {
        public static void install()
        {
            TypePool typePool = TypePool.Default.ofSystemLoader();
            new ByteBuddy()
            .rebase(typePool.describe("org.apache.cassandra.spark.bulkwriter.SortedSSTableWriter").resolve(),
                    ClassFileLocator.ForClassLoader.ofSystemLoader())
            .method(named("validateSSTables").and(takesArguments(BulkWriterContext.class, Path.class, Set.class)))
            .intercept(MethodDelegation.to(BBHelperInterceptSortedSSTableWriterValidateSSTables.class))
            .make()
            .load(BBHelperInterceptSortedSSTableWriterValidateSSTables.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void validateSSTables(BulkWriterContext context,
                                            Path outputDirectory,
                                            Set<Path> dataFilePaths,
                                            @SuperCall Callable<?> orig) throws Exception
        {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(outputDirectory, "*Data.db"))
            {
                Path dataFile = stream.iterator().next();
                try (RandomAccessFile file = new RandomAccessFile(dataFile.toFile(), "rw"))
                {
                    file.seek(file.length() / 2);
                    file.writeChars("THIS IS CORRUPT DATA AND SHOULD NOT BE READABLE");
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
            orig.call();
        }
    }
}
