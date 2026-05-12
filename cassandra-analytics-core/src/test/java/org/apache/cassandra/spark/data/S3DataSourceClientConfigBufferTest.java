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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.backup.BackupReaderRegistry;
import org.apache.cassandra.spark.data.backup.FakeBackupReader;
import org.apache.cassandra.spark.utils.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the new DataSource options on {@link S3DataSourceClientConfig}:
 * Data.db buffer sizing and SSTable metadata cache sizing. These are the user-visible knobs that
 * determine how many S3 GETs (and KMS decrypts) a given Spark job will issue, so regressions that
 * silently fall back to the tiny legacy defaults would immediately erode the gains from the
 * optimization. Serialization/Kryo coverage for the buffer-size round trip lives in
 * {@link S3CassandraDataLayerTests}.
 */
class S3DataSourceClientConfigBufferTest
{
    private static final String TEST_BACKUP_READER_TYPE = "fake";

    // Sysprops we may touch inside applySSTableCacheSystemProperties() — remembered here so each
    // test cleans up after itself and does not leak into sibling tests running in the same JVM.
    private static final String[] CACHE_SYSPROPS = {
        "sbr.cache.summary.maxEntries",
        "sbr.cache.index.maxEntries",
        "sbr.cache.stats.maxEntries",
        "sbr.cache.filter.maxEntries",
        "sbr.cache.compressionInfo.maxEntries"
    };

    @BeforeAll
    static void registerBackupReader()
    {
        BackupReaderRegistry.register(TEST_BACKUP_READER_TYPE, config -> new FakeBackupReader(config.s3Config(), config.s3Config().s3Bucket()));
    }

    @AfterEach
    void clearCacheSysprops()
    {
        for (String name : CACHE_SYSPROPS)
        {
            System.clearProperty(name);
        }
    }

    private static Map<String, String> minimalOptions()
    {
        // Case-sensitivity: MapUtils.lowerCaseKey() lower-cases the keys, so we store lower-cased
        // forms here to match the production option path. Real Spark DataSource options are already
        // case-insensitive from the caller's perspective.
        Map<String, String> options = new HashMap<>();
        // The cluster identifier is opaque to the buffer-size path we are testing.
        options.put("clusterid", UUID.randomUUID().toString());
        options.put("keyspace", "ks");
        options.put("table", "tbl");
        options.put("tablecreatestmt", "CREATE TABLE ks.tbl (k int PRIMARY KEY)");
        options.put("s3-region", "us-west-2");
        options.put("s3-bucket", "bucket");
        options.put("backupreadertype", TEST_BACKUP_READER_TYPE);
        return options;
    }

    @Test
    void defaultsMatchPropertiesConstants()
    {
        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(minimalOptions());

        assertEquals(Properties.DEFAULT_S3_DATA_CHUNK_BUFFER_SIZE, config.s3DataChunkBufferSize());
        assertEquals(Properties.DEFAULT_S3_DATA_MAX_BUFFER_SIZE,   config.s3DataMaxBufferSize());
        assertTrue(config.s3DataMaxBufferSize() >= config.s3DataChunkBufferSize(),
                   "max buffer must be >= chunk buffer so BufferingInputStream can enqueue a full chunk");

        // Data.db publisher-read path defaults to enabled.
        assertTrue(config.sstableDataPublisherReadEnabled(),
                   "sstableDataPublisherReadEnabled must default true to use the streaming path");

        // Cache defaults — aligned with SSTableCache bump.
        assertEquals(32768, config.sstableCacheSummaryMaxEntries());
        assertEquals(16384, config.sstableCacheIndexMaxEntries());
        assertEquals(16384, config.sstableCacheStatsMaxEntries());
        assertEquals(16384, config.sstableCacheFilterMaxEntries());
        assertEquals(16384, config.sstableCacheCompressionInfoMaxEntries());
    }

    @Test
    void sstableDataPublisherReadEnabledExplicitTrueIsHonored()
    {
        Map<String, String> options = minimalOptions();
        options.put("sstabledatapublisherreadenabled", "true");

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);

        assertTrue(config.sstableDataPublisherReadEnabled(),
                   "explicit true must enable the AsyncResponseTransformer.toPublisher() experiment path");
    }

    @Test
    void sstableDataPublisherReadEnabledExplicitFalseIsHonored()
    {
        Map<String, String> options = minimalOptions();
        options.put("sstabledatapublisherreadenabled", "false");

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);

        assertFalse(config.sstableDataPublisherReadEnabled(),
                    "explicit false must keep the toBytes() baseline path for Data.db");
    }

    @Test
    void overridesAreHonored()
    {
        // Use values distinct from the 8/32 MiB defaults to ensure the override path actually flows
        // through, not just a coincidental match with the constants.
        Map<String, String> options = minimalOptions();
        options.put("sstabledatachunkbuffersize", String.valueOf(16L * 1024 * 1024));
        options.put("sstabledatamaxbuffersize",   String.valueOf(48L * 1024 * 1024));
        options.put("sstablecachesummarymaxentries",          "4096");
        options.put("sstablecachecompressioninfomaxentries",  "2048");

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);

        assertEquals(16L * 1024 * 1024, config.s3DataChunkBufferSize());
        assertEquals(48L * 1024 * 1024, config.s3DataMaxBufferSize());
        assertEquals(4096, config.sstableCacheSummaryMaxEntries());
        assertEquals(2048, config.sstableCacheCompressionInfoMaxEntries());
    }

    @Test
    void maxBufferIsClampedToAtLeastChunkSize()
    {
        // Users who supply a maxBuffer smaller than the chunk size would break the BufferingInputStream
        // invariant (one chunk must fit without immediately tripping isBufferFull()). Verify the
        // defensive clamp we apply in the constructor.
        Map<String, String> options = minimalOptions();
        options.put("sstabledatachunkbuffersize", String.valueOf(64L * 1024 * 1024));
        options.put("sstabledatamaxbuffersize",   String.valueOf(1L * 1024 * 1024));

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);

        assertEquals(64L * 1024 * 1024, config.s3DataChunkBufferSize());
        assertEquals(64L * 1024 * 1024, config.s3DataMaxBufferSize(),
                     "maxBuffer must be clamped up to at least chunk size");
    }

    @Test
    void chunkSizeHasMinimumOneMebibyte()
    {
        // Defense-in-depth: an accidentally tiny override would nerf throughput severely. 1 MiB is a
        // conservative floor — still smaller than any observed Data.db component — so it is safe to
        // clamp rather than throw.
        Map<String, String> options = minimalOptions();
        options.put("sstabledatachunkbuffersize", "0");

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);

        assertEquals(1024L * 1024L, config.s3DataChunkBufferSize());
    }

    @Test
    void applySSTableCacheSystemPropertiesSetsUnsetSysprops()
    {
        for (String name : CACHE_SYSPROPS)
        {
            assertTrue(System.getProperty(name) == null,
                       "precondition: cache sysprop " + name + " should not be set prior to the test");
        }

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(minimalOptions());
        config.applySSTableCacheSystemProperties();

        assertEquals("32768", System.getProperty("sbr.cache.summary.maxEntries"));
        assertEquals("16384", System.getProperty("sbr.cache.index.maxEntries"));
        assertEquals("16384", System.getProperty("sbr.cache.stats.maxEntries"));
        assertEquals("16384", System.getProperty("sbr.cache.filter.maxEntries"));
        assertEquals("16384", System.getProperty("sbr.cache.compressionInfo.maxEntries"));
    }

    @Test
    void negativeCacheSizeFailsFastWithOptionNameInError()
    {
        // Negative max-entries values would otherwise reach Guava CacheBuilder.maximumSize() and surface
        // as a generic IllegalArgumentException ("size cannot be negative") with no mention of which
        // option the operator misconfigured. Catching this at config-parse time with the option key in
        // the message is dramatically more debuggable.
        Map<String, String> options = minimalOptions();
        options.put("sstablecacheindexmaxentries", "-1");

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> S3DataSourceClientConfig.create(options));
        assertTrue(ex.getMessage().contains("sstableCacheIndexMaxEntries"),
                   "error message must include the offending option key, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("-1"),
                   "error message must echo the offending value, was: " + ex.getMessage());
    }

    @Test
    void zeroCacheSizeIsAccepted()
    {
        // 0 is a legitimate way to disable the cache entirely (CacheBuilder.maximumSize(0) is valid).
        // Make sure the fast-fail validation does not swing too hard and reject this case.
        Map<String, String> options = minimalOptions();
        options.put("sstablecachesummarymaxentries", "0");

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(options);
        assertEquals(0, config.sstableCacheSummaryMaxEntries());
    }

    @Test
    void applySSTableCacheSystemPropertiesDoesNotOverwriteExistingSysprops()
    {
        // Operators that ship cluster-wide -D flags should see their values win over the defaults —
        // otherwise a fleet-wide deploy could silently revert their tuning.
        System.setProperty("sbr.cache.summary.maxEntries", "65536");

        S3DataSourceClientConfig config = S3DataSourceClientConfig.create(minimalOptions());
        config.applySSTableCacheSystemProperties();

        assertEquals("65536", System.getProperty("sbr.cache.summary.maxEntries"),
                     "operator-set sysprops must win over config defaults");
    }
}
