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

package org.apache.cassandra.analytics;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Morph;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Verifies the bulk reader can read a table whose on-disk SSTables span more than one BIG version.
 *
 * <p>On a single Cassandra 5.x cluster the bulk writer normally produces SSTables in a single version. To
 * force a mix, {@link org.apache.cassandra.bridge.SSTableVersionAnalyzer#determineBridgeVersionForWrite} is
 * intercepted so the first write is driven by the {@code FOURZERO} bridge (producing {@code big-nb} SSTables)
 * and the second by the {@code FIVEZERO} bridge (producing {@code big-oa} SSTables). The reader must then
 * select the highest version present ({@code FIVEZERO}), which can read both, and return all rows.
 *
 * <p>Finally the override is cleared and a third write is performed with the real, gossip-driven
 * determination: because the cluster now advertises both {@code big-nb} and {@code big-oa}, the writer must
 * select the <em>lowest</em> mutually-compatible version ({@code FOURZERO}) so the produced SSTables remain
 * importable by every node, which is verified by asserting the newly written files are {@code big-nb}.
 */
public class MixedSSTableVersionTest extends SharedClusterSparkIntegrationTestBase
{
    static QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);

    static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
    DataTypes.createStructField("id", DataTypes.IntegerType, false),
    DataTypes.createStructField("name", DataTypes.StringType, false)
    });

    static CassandraVersion bridgeVersion = null;

    int sequence = 0;

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion().isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("Test requires Cassandra 5.x to read sstables in BIG 'nb' and 'oa' versions")
        .isTrue();
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        interceptBridgeDeterminationForWrite();
    }

    @Override
    protected void afterClusterShutdown()
    {
        super.afterClusterShutdown();
        resetInterception();
    }

    @Test
    public void writeAndReadDifferentBigVersions()
    {
        // write data as big-nb sstables
        bridgeVersion = CassandraVersion.FOURZERO;
        Dataset<Row> rows1 = writeDataAndFlush();

        // write data as big-oa sstables
        bridgeVersion = CassandraVersion.FIVEZERO;
        Dataset<Row> rows2 = writeDataAndFlush();

        Set<String> dataFiles = findSSTableDataFiles(cluster.get(1), table1);
        // check that we produced data files in two different BIG versions
        assertThat(dataFiles.stream().filter(name -> name.startsWith("nb-"))).isNotEmpty();
        assertThat(dataFiles.stream().filter(name -> name.startsWith("oa-"))).isNotEmpty();

        // read the data back through the bulk reader
        // FIVEZERO bridge should be used
        Dataset<Row> dfRead = bulkReaderDataFrame(table1).load();
        checkSmallDataFrameEquality(rows1.union(rows2), dfRead);

        // Now let the writer determine the bridge for real (no override). The cluster advertises both
        // big-nb (4.0) and big-oa (5.0), so determineBridgeVersionForWrite must pick the LOWEST
        // mutually-compatible version (4.0) so the produced SSTables can be imported by every node.
        // Verify the newly written SSTables are big-nb, proving the writer picked FOURZERO.
        bridgeVersion = null;
        Set<String> filesBeforeThirdWrite = findSSTableDataFiles(cluster.get(1), table1);
        Dataset<Row> rows3 = writeDataAndFlush();

        Set<String> newFiles = new HashSet<>(findSSTableDataFiles(cluster.get(1), table1));
        newFiles.removeAll(filesBeforeThirdWrite);
        assertThat(newFiles)
        .as("third write should produce new SSTable data files")
        .isNotEmpty();
        assertThat(newFiles)
        .as("writer must pick the lowest mutually-compatible version present (4.0 -> big-nb): %s", newFiles)
        .allMatch(name -> name.startsWith("nb-"));

        // The bulk reader still returns every row across all three writes.
        Dataset<Row> dfReadAll = bulkReaderDataFrame(table1).load();
        checkSmallDataFrameEquality(rows1.union(rows2).union(rows3), dfReadAll);
    }

    private Dataset<Row> writeDataAndFlush()
    {
        List<Row> data = generateRandomData();
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = spark.createDataFrame(data, SCHEMA);
        bulkWriterDataFrameWriter(dfWrite, table1).save();
        cluster.stream().forEach(instance -> instance.nodetool("flush", TEST_KEYSPACE));
        return dfWrite;
    }

    private List<Row> generateRandomData()
    {
        int rowCount = RandomUtils.nextInt(3, 10);
        List<Row> data = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            data.add(RowFactory.create(sequence++, UUID.randomUUID().toString()));
        }
        return data;
    }

    @RuntimeType
    @SuppressWarnings("unused")
    public static CassandraVersion determineBridgeVersionForWrite(Set<String> sstableVersionsOnCluster,
                                                                  String requestedFormat,
                                                                  @Morph MorphBridge bridge)
    {
        if (bridgeVersion == null)
        {
            Object[] args = new Object[]{sstableVersionsOnCluster, requestedFormat};
            return (CassandraVersion) bridge.invokeOriginal(args);
        }
        return bridgeVersion;
    }

    /**
     * Intercept {@code SSTableVersionAnalyzer#determineBridgeVersionForWrite()} to force creation of
     * BIG sstables in different versions.
     */
    private void interceptBridgeDeterminationForWrite()
    {
        ClassLoader cl = getClass().getClassLoader();
        TypePool typePool = TypePool.Default.of(cl);
        TypeDescription description = typePool.describe("org.apache.cassandra.bridge.SSTableVersionAnalyzer")
                                              .resolve();

        new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                       .method(named("determineBridgeVersionForWrite"))
                       .intercept(MethodDelegation.withDefaultConfiguration()
                                                  .withBinders(Morph.Binder.install(MorphBridge.class))
                                                  .to(MixedSSTableVersionTest.class))
                       .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                       .load(cl, ClassLoadingStrategy.Default.INJECTION);
    }

    private void resetInterception()
    {
        bridgeVersion = null;
    }

    public interface MorphBridge
    {
        Object invokeOriginal(Object[] args);
    }
}
