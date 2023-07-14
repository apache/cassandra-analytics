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

import org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.spark.SparkConf;
import org.jetbrains.annotations.NotNull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BulkSparkConfTest
{
    private SparkConf sparkConf;
    private BulkSparkConf bulkSparkConf;
    private Map<String, String> defaultOptions;

    @BeforeEach
    public void before()
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
    public void testGetBoolean()
    {
        sparkConf.set("spark.cassandra_analytics.job.skip_clean", "true");
        assertThat(bulkSparkConf.getSkipClean(), is(true));
    }

    @Test
    public void testGetLong()
    {
        sparkConf.set("spark.cassandra_analytics.sidecar.request.retries.delay.seconds", "2222");
        assertThat(bulkSparkConf.getSidecarRequestRetryDelayInSeconds(), is(2222L));
    }

    @Test
    public void testGetInt()
    {
        sparkConf.set("spark.cassandra_analytics.request.max_connections", "1234");
        assertThat(bulkSparkConf.getMaxHttpConnections(), is(1234));
    }

    @Test
    public void deprecatedSettingsAreHonored()
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

        assertThat(bulkSparkConf.getMaxHttpConnections(), is(1234));
        assertThat(bulkSparkConf.getHttpResponseTimeoutMs(), is(5678));
    }

    @Test
    public void calculatesCoresCorrectlyForStaticAllocation()
    {
        sparkConf.set("spark.executor.cores", "5");
        sparkConf.set("spark.executor.instances", "5");
        assertThat(bulkSparkConf.getCores(), is(25));
    }

    @Test
    public void calculatesCoresCorrectlyForDynamicAllocation()
    {
        sparkConf.set("spark.executor.cores", "6");
        sparkConf.set("spark.dynamicAllocation.maxExecutors", "7");
        assertThat(bulkSparkConf.getCores(), is(42));
    }

    @Test
    public void ensureSetupSparkConfAddsPerformsNecessaryTasks()
    {
        assertThat(sparkConf.get("spark.kryo.registrator", ""), is(emptyString()));
        assertThat(sparkConf.get("spark.executor.extraJavaOptions", ""), is(emptyString()));
        BulkSparkConf.setupSparkConf(sparkConf, true);
        assertEquals("," + SbwKryoRegistrator.class.getName(), sparkConf.get("spark.kryo.registrator", ""));
        if (BuildInfo.isAtLeastJava11(BuildInfo.javaSpecificationVersion()))
        {
            assertEquals(BulkSparkConf.JDK11_OPTIONS, sparkConf.get("spark.executor.extraJavaOptions", ""));
        }
    }

    @Test
    public void withProperMtlsSettingsWillCreateSuccessfully()
    {
        // mTLS is now required, and the BulkSparkConf constructor fails if the options aren't present
        Map<String, String> options = copyDefaultOptions();
        SparkConf sparkConf = new SparkConf();
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, options);
    }

    @Test
    public void keystorePathRequiredIfBase64EncodedKeystoreNotSet()
    {
        Map<String, String> options = copyDefaultOptions();
        options.remove(WriterOptions.KEYSTORE_PATH.name());
        SparkConf sparkConf = new SparkConf();
        NullPointerException npe = assertThrows(NullPointerException.class,
                                                () -> new BulkSparkConf(sparkConf, options));
        assertEquals("Keystore password was set. But both keystore path and base64 encoded string are not set. "
                     + "Please either set option " + WriterOptions.KEYSTORE_PATH
                     + " or option " + WriterOptions.KEYSTORE_BASE64_ENCODED, npe.getMessage());
    }

    @Test
    public void testSkipClean()
    {
        assertFalse(bulkSparkConf.getSkipClean());
        sparkConf.set(BulkSparkConf.SKIP_CLEAN, "true");
        assertTrue(bulkSparkConf.getSkipClean());
    }

    @Test
    public void testDefaultSidecarPort()
    {
        bulkSparkConf = new BulkSparkConf(new SparkConf(), defaultOptions);
        assertEquals(9043, bulkSparkConf.getSidecarPort());
    }

    @Test
    public void testSidecarPortSetByOptions()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.SIDECAR_PORT.name(), "9999");
        bulkSparkConf = new BulkSparkConf(new SparkConf(), options);
        assertEquals(9999, bulkSparkConf.getSidecarPort());
    }

    @Test
    public void testSidecarPortSetByProperty()
    {
        // Spark conf loads values from system properties, but we can also test by calling `.set` explicitly.
        // This makes the test not pollute global (System.properties) state but still tests the same basic path.
        SparkConf conf = new SparkConf()
                         .set(BulkSparkConf.SIDECAR_PORT, "9876");
        bulkSparkConf = new BulkSparkConf(conf, defaultOptions);
        assertEquals(9876, bulkSparkConf.getSidecarPort());
    }

    @Test
    public void testKeystoreBase64EncodedStringSet()
    {
        Map<String, String> options = copyDefaultOptions();
        options.remove(WriterOptions.KEYSTORE_PATH.name());
        options.put(WriterOptions.KEYSTORE_BASE64_ENCODED.name(), "dummy_base64_encoded_keystore");
        bulkSparkConf = new BulkSparkConf(sparkConf, defaultOptions);
    }

    @Test
    public void testTrustStorePasswordSetPathNotSet()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.TRUSTSTORE_PATH.name(), "dummy");
        NullPointerException npe = assertThrows(NullPointerException.class,
                                                () -> new BulkSparkConf(sparkConf, options));
        assertEquals("Trust Store Path was provided, but password is missing. "
                     + "Please provide option " + WriterOptions.TRUSTSTORE_PASSWORD, npe.getMessage());
    }

    @Test
    public void testUnbufferedRowBufferMode()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.ROW_BUFFER_MODE.name(), "UNBUFFERED");
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, options);
        assertNotNull(bulkSparkConf);
        assertEquals(bulkSparkConf.rowBufferMode, RowBufferMode.UNBUFFERED);
    }

    @Test
    public void testBufferedRowBufferMode()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.ROW_BUFFER_MODE.name(), "BUFFERED");
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, options);
        assertNotNull(bulkSparkConf);
        assertEquals(bulkSparkConf.rowBufferMode, RowBufferMode.BUFFERED);
    }

    @Test
    public void testInvalidRowBufferMode()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.ROW_BUFFER_MODE.name(), "invalid");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                                                          () -> new BulkSparkConf(sparkConf, options));
        assertEquals("Key row buffering mode with value invalid is not a valid Enum of type class org.apache.cassandra.spark.bulkwriter.RowBufferMode.",
                     exception.getMessage());
    }

    @Test
    public void testBufferedRowBufferModeWithZeroBatchSize()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.ROW_BUFFER_MODE.name(), "BUFFERED");
        options.put(WriterOptions.BATCH_SIZE.name(), "0");
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, options);
        assertNotNull(bulkSparkConf);
        assertEquals(bulkSparkConf.rowBufferMode, RowBufferMode.BUFFERED);
    }

    @Test
    public void testNonZeroBatchSizeIsIgnoredWithBufferedRowBufferMode()
    {
        Map<String, String> options = copyDefaultOptions();
        options.put(WriterOptions.BATCH_SIZE.name(), "5");
        options.put(WriterOptions.ROW_BUFFER_MODE.name(), "BUFFERED");
        BulkSparkConf bulkSparkConf = new BulkSparkConf(sparkConf, options);
        assertNotNull(bulkSparkConf);
        assertEquals(bulkSparkConf.rowBufferMode, RowBufferMode.BUFFERED);
    }

    private Map<String, String> copyDefaultOptions()
    {
        TreeMap<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        map.putAll(defaultOptions);
        return map;
    }
}
