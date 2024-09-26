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

package org.apache.cassandra.spark.bulkwriter;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.spark.SparkConf;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.bulkwriter.BulkSparkConf.DEFAULT_SIDECAR_PORT;
import static org.apache.cassandra.spark.bulkwriter.BulkSparkConf.MINIMUM_JOB_KEEP_ALIVE_MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BulkSparkConfTest
{
    private SparkConf sparkConf;
    private BulkSparkConf bulkSparkConf;
    private Map<String, String> defaultOptions;

    @BeforeEach
    void before()
    {
        sparkConf = new SparkConf();
        defaultOptions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        defaultOptions.put(WriterOptions.SIDECAR_INSTANCES.name(), "127.0.0.1");
        defaultOptions.put(WriterOptions.KEYSPACE.name(), "ks");
        defaultOptions.put(WriterOptions.TABLE.name(), "table");
        defaultOptions.put(WriterOptions.KEYSTORE_PASSWORD.name(), "dummy_password");
        defaultOptions.put(WriterOptions.KEYSTORE_PATH.name(), "dummy_path");
        bulkSparkConf = new BulkSparkConf(sparkConf, defaultOptions);
    }

    @Test
    void testGetBoolean()
    {
        sparkConf.set("spark.cassandra_analytics.job.skip_clean", "true");
        assertThat(bulkSparkConf.getSkipClean()).isTrue();
    }

    @Test
    void testGetLong()
    {
        sparkConf.set("spark.cassandra_analytics.sidecar.request.retries.delay.milliseconds", "2222");
        assertThat(bulkSparkConf.getSidecarRequestRetryDelayMillis()).isEqualTo(2222L);
    }

    @Test
    void testGetInt()
    {
        sparkConf.set("spark.cassandra_analytics.request.max_connections", "1234");
        assertThat(bulkSparkConf.getMaxHttpConnections()).isEqualTo(1234);
    }

    @Test
    void deprecatedSettingsAreHonored()
    {
        // Test that deprecated names of settings are in fact picked up correctly

        sparkConf.set("qwerty.request.max_connections", "1234");
        sparkConf.set("asdfgh.request.max_connections", "000000");
        sparkConf.set("zxcvbn.request.response_timeout", "5678");
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, defaultOptions)
        {
            @Override
            @NotNull
            protected List<String> getDeprecatedSettingPrefixes()
            {
                return ImmutableList.of("qwerty.", "asdfgh.", "zxcvbn.");
            }
        };

        assertThat(bulkSparkConf.getMaxHttpConnections()).isEqualTo(1234);
        assertThat(bulkSparkConf.getHttpResponseTimeoutMs()).isEqualTo(5678);
    }

    @Test
    void calculatesCoresCorrectlyForStaticAllocation()
    {
        sparkConf.set("spark.executor.cores", "5");
        sparkConf.set("spark.executor.instances", "5");
        assertThat(bulkSparkConf.getCores()).isEqualTo(25);
    }

    @Test
    void calculatesCoresCorrectlyForDynamicAllocation()
    {
        sparkConf.set("spark.executor.cores", "6");
        sparkConf.set("spark.dynamicAllocation.maxExecutors", "7");
        assertThat(bulkSparkConf.getCores()).isEqualTo(42);
    }

    @Test
    void ensureSetupSparkConfAddsPerformsNecessaryTasks()
    {
        assertThat(sparkConf.get("spark.kryo.registrator", "")).isEmpty();
        assertThat(sparkConf.get("spark.executor.extraJavaOptions", "")).isEmpty();
        BulkSparkConf.setupSparkConf(sparkConf, true);
        assertThat(sparkConf.get("spark.kryo.registrator", ""))
        .isEqualTo("," + SbwKryoRegistrator.class.getName());
        if (BuildInfo.isAtLeastJava11(BuildInfo.javaSpecificationVersion()))
        {
            assertThat(sparkConf.get("spark.executor.extraJavaOptions", ""))
            .isEqualTo(BulkSparkConf.JDK11_OPTIONS);
        }
    }

    @Test
    void withProperMtlsSettingsWillCreateSuccessfully()
    {
        // mTLS is now required, and the BulkSparkConf constructor fails if the options aren't present
        Map<String, String> options = copyDefaultOptions();
        SparkConf sparkConf = new SparkConf();
        assertThatNoException().isThrownBy(() -> new BulkSparkConf(sparkConf, options));
    }

    @Test
    void keystorePathRequiredIfBase64EncodedKeystoreNotSet()
    {
        Map<String, String> options = copyDefaultOptions();
        options.remove(WriterOptions.KEYSTORE_PATH.name());
        SparkConf sparkConf = new SparkConf();
        assertThatThrownBy(() -> new BulkSparkConf(sparkConf, options))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("Keystore password was set. But both keystore path and base64 encoded string are not set. "
                    + "Please either set option " + WriterOptions.KEYSTORE_PATH
                    + " or option " + WriterOptions.KEYSTORE_BASE64_ENCODED);
    }

    @Test
    void testSkipClean()
    {
        assertThat(bulkSparkConf.getSkipClean()).isFalse();
        sparkConf.set(BulkSparkConf.SKIP_CLEAN, "true");
        assertThat(bulkSparkConf.getSkipClean()).isTrue();
    }

    @Test
    void testDefaultSidecarPort()
    {
        bulkSparkConf = new BulkSparkConf(new SparkConf(), defaultOptions);
        assertThat(bulkSparkConf.getUserProvidedSidecarPort()).isEqualTo(-1);
        assertThat(bulkSparkConf.getEffectiveSidecarPort()).isEqualTo(DEFAULT_SIDECAR_PORT);
    }

    @Test
    void testSidecarPortSetByOptions()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.SIDECAR_PORT.name(), "9999");
        bulkSparkConf = new BulkSparkConf(new SparkConf(), options);
        assertThat(bulkSparkConf.getUserProvidedSidecarPort()).isEqualTo(9999);
        assertThat(bulkSparkConf.getEffectiveSidecarPort()).isEqualTo(9999);
    }

    @Test
    void testSidecarPortSetByProperty()
    {
        // Spark conf loads values from system properties, but we can also test by calling `.set` explicitly.
        // This makes the test not pollute global (System.properties) state but still tests the same basic path.
        SparkConf conf = new SparkConf()
                         .set(BulkSparkConf.SIDECAR_PORT, "9876");
        bulkSparkConf = new BulkSparkConf(conf, defaultOptions);
        assertThat(bulkSparkConf.getUserProvidedSidecarPort()).isEqualTo(9876);
        assertThat(bulkSparkConf.getEffectiveSidecarPort()).isEqualTo(9876);
    }

    @Test
    void testKeystoreBase64EncodedStringSet()
    {
        Map<String, String> options = copyDefaultOptions();
        options.remove(WriterOptions.KEYSTORE_PATH.name());
        options.put(WriterOptions.KEYSTORE_BASE64_ENCODED.name(), "dummy_base64_encoded_keystore");
        assertThatNoException().isThrownBy(() -> new BulkSparkConf(sparkConf, defaultOptions));
    }

    @Test
    void testTrustStorePasswordSetPathNotSet()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.TRUSTSTORE_PATH.name(), "dummy");
        assertThatThrownBy(() -> new BulkSparkConf(sparkConf, options))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("Trust Store Path was provided, but password is missing. "
                    + "Please provide option " + WriterOptions.TRUSTSTORE_PASSWORD);
    }

    @Test
    void testQuoteIdentifiers()
    {
        assertThat(bulkSparkConf.quoteIdentifiers).isFalse();
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.QUOTE_IDENTIFIERS.name(), "true");
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, options);
        assertThat(bulkSparkConf).isNotNull();
        assertThat(bulkSparkConf.quoteIdentifiers).isTrue();
    }

    @Test
    void testInvalidJobKeepAliveMinutes()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.JOB_KEEP_ALIVE_MINUTES.name(), "-100");
        assertThatThrownBy(() -> new BulkSparkConf(sparkConf, options))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value for the 'JOB_KEEP_ALIVE_MINUTES' Bulk Writer option (-100). It cannot be less than the minimum 10");
    }

    @Test
    void testDefaultJobKeepAliveMinutes()
    {
        Map<String, String> options = copyDefaultOptions();
        BulkSparkConf conf = new BulkSparkConf(sparkConf, options);
        assertThat(conf.getJobKeepAliveMinutes()).isEqualTo(MINIMUM_JOB_KEEP_ALIVE_MINUTES);
    }

    @Test
    void testJobKeepAliveMinutes()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.JOB_KEEP_ALIVE_MINUTES.name(), "30");
        BulkSparkConf conf = new BulkSparkConf(sparkConf, options);
        assertThat(conf.getJobKeepAliveMinutes()).isEqualTo(30);
    }

    @Test
    void testSidecarContactPoints()
    {
        Map<String, String> options = copyDefaultOptions();
        assertThat(BulkSparkConf.resolveSidecarContactPoints(options)).isEqualTo("127.0.0.1");

        String sidecarInstances = "127.0.0.1,128.0.0.2";
        options.put(WriterOptions.SIDECAR_INSTANCES.name(), sidecarInstances);
        assertThat(BulkSparkConf.resolveSidecarContactPoints(options)).isEqualTo(sidecarInstances);

        String contactPoints = "localhost1,localhost2";
        options.put(WriterOptions.SIDECAR_CONTACT_POINTS.name(), contactPoints);
        assertThat(BulkSparkConf.resolveSidecarContactPoints(options)).isEqualTo(contactPoints);

        String contactPointsWithPort = "localhost1:9999,localhost2:9999";
        options.put(WriterOptions.SIDECAR_CONTACT_POINTS.name(), contactPointsWithPort);
        assertThat(BulkSparkConf.resolveSidecarContactPoints(options)).isEqualTo(contactPointsWithPort);

        options.remove(WriterOptions.SIDECAR_INSTANCES.name());
        options.remove(WriterOptions.SIDECAR_CONTACT_POINTS.name());
        assertThat(BulkSparkConf.resolveSidecarContactPoints(options))
        .describedAs("When none of the sidecar options are define. It resolves to the default value null")
        .isNull();
    }

    @Test
    void testReadCoordinatedWriteConfFails()
    {
        Map<String, String> options = copyDefaultOptions();
        String coordinatedWriteConfJsonNoLocalDc = "{\"cluster1\":" +
                                                   "{\"sidecarContactPoints\":[\"instance-1:9999\",\"instance-2:9999\",\"instance-3:9999\"]}}";

        options.put(WriterOptions.COORDINATED_WRITE_CONFIG.name(), coordinatedWriteConfJsonNoLocalDc);
        assertThatThrownBy(() -> new BulkSparkConf(sparkConf, options))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Coordinated write only supports S3_COMPAT");

        options.put(WriterOptions.DATA_TRANSPORT.name(), DataTransport.S3_COMPAT.name());
        options.put(WriterOptions.BULK_WRITER_CL.name(), "LOCAL_QUORUM");
        assertThatThrownBy(() -> new BulkSparkConf(sparkConf, options))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("localDc is not configured for cluster: cluster1 for consistency level: LOCAL_QUORUM");

        options.put(WriterOptions.COORDINATED_WRITE_CONFIG.name(), "invalid json");
        assertThatThrownBy(() -> new BulkSparkConf(sparkConf, options))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to parse json string into CoordinatedWriteConf of SimpleClusterConf due to Unrecognized token 'invalid'");
    }

    @Test
    void testCoordinatedWriteConf()
    {
        Map<String, String> options = copyDefaultOptions();
        options.remove(WriterOptions.COORDINATED_WRITE_CONFIG.name());
        BulkSparkConf conf = new BulkSparkConf(sparkConf, options);
        assertThat(conf.isCoordinatedWriteConfigured())
        .describedAs("When COORDINATED_WRITE_CONF is absent, isCoordinatedWriteConfigured should return false")
        .isFalse();

        String coordinatedWriteConfJson = "{\"cluster1\":" +
                                          "{\"sidecarContactPoints\":[\"instance-1:9999\",\"instance-2:9999\",\"instance-3:9999\"]," +
                                          "\"localDc\":\"dc1\"}," +
                                          "\"cluster2\":" +
                                          "{\"sidecarContactPoints\":[\"instance-4:8888\",\"instance-5:8888\",\"instance-6:8888\"]," +
                                          "\"localDc\":\"dc1\"}}";
        options.put(WriterOptions.DATA_TRANSPORT.name(), DataTransport.S3_COMPAT.name());
        options.put(WriterOptions.BULK_WRITER_CL.name(), "LOCAL_QUORUM");
        options.put(WriterOptions.COORDINATED_WRITE_CONFIG.name(), coordinatedWriteConfJson);
        conf = new BulkSparkConf(sparkConf, options);
        assertThat(conf.isCoordinatedWriteConfigured())
        .describedAs("When COORDINATED_WRITE_CONF is present, it should return true")
        .isTrue();
        assertThat(conf.coordinatedWriteConf().clusters()).containsOnlyKeys("cluster1", "cluster2");
        CoordinatedWriteConf.ClusterConf cluster1 = conf.coordinatedWriteConf().cluster("cluster1");
        assertThat(cluster1).isNotNull();
        assertThat(cluster1.sidecarContactPoints())
        .containsExactlyInAnyOrder(new SidecarInstanceImpl("instance-1", 9999),
                                   new SidecarInstanceImpl("instance-2", 9999),
                                   new SidecarInstanceImpl("instance-3", 9999));
        assertThat(cluster1.localDc()).isEqualTo("dc1");
        CoordinatedWriteConf.ClusterConf cluster2 = conf.coordinatedWriteConf().cluster("cluster2");
        assertThat(cluster2).isNotNull();
        assertThat(cluster2.sidecarContactPoints())
        .containsExactlyInAnyOrder(new SidecarInstanceImpl("instance-4", 8888),
                                   new SidecarInstanceImpl("instance-5", 8888),
                                   new SidecarInstanceImpl("instance-6", 8888));
        assertThat(cluster2.localDc()).isEqualTo("dc1");
    }

    private Map<String, String> copyDefaultOptions()
    {
        TreeMap<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        map.putAll(defaultOptions);
        return map;
    }
}
