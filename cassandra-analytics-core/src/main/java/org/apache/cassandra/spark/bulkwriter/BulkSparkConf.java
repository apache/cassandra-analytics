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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.RowBufferMode;
import org.apache.cassandra.clients.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator;
import org.apache.cassandra.spark.utils.BuildInfo;
import org.apache.cassandra.spark.utils.MapUtils;
import org.apache.spark.SparkConf;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("WeakerAccess")
public class BulkSparkConf implements Serializable
{
    private static final long serialVersionUID = -5060973521517656241L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkSparkConf.class);

    public static final String JDK11_OPTIONS = " -Djdk.attach.allowAttachSelf=true"
                                             + " --add-exports java.base/jdk.internal.misc=ALL-UNNAMED"
                                             + " --add-exports java.base/jdk.internal.ref=ALL-UNNAMED"
                                             + " --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
                                             + " --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED"
                                             + " --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED"
                                             + " --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED"
                                             + " --add-exports java.sql/java.sql=ALL-UNNAMED"
                                             + " --add-opens java.base/java.lang.module=ALL-UNNAMED"
                                             + " --add-opens java.base/jdk.internal.loader=ALL-UNNAMED"
                                             + " --add-opens java.base/jdk.internal.ref=ALL-UNNAMED"
                                             + " --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED"
                                             + " --add-opens java.base/jdk.internal.math=ALL-UNNAMED"
                                             + " --add-opens java.base/jdk.internal.module=ALL-UNNAMED"
                                             + " --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED"
                                             + " --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED";

    public static final int DEFAULT_NUM_SPLITS = -1;
    public static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 100_000;
    public static final int DEFAULT_HTTP_RESPONSE_TIMEOUT = 100_000;
    public static final int DEFAULT_HTTP_MAX_CONNECTIONS = 200;
    public static final int DEFAULT_SIDECAR_PORT = 9043;
    public static final int DEFAULT_SIDECAR_REQUEST_RETRIES = 10;
    public static final long DEFAULT_SIDECAR_REQUEST_RETRY_DELAY_SECONDS = 1L;
    public static final long DEFAULT_SIDECAR_REQUEST_MAX_RETRY_DELAY_SECONDS = 60L;
    public static final int DEFAULT_COMMIT_BATCH_SIZE = 10_000;
    public static final int DEFAULT_RING_RETRY_COUNT = 3;
    public static final RowBufferMode DEFAULT_ROW_BUFFER_MODE = RowBufferMode.UNBUFFERED;
    public static final int DEFAULT_BATCH_SIZE_IN_ROWS = 1_000_000;

    // NOTE: All Cassandra Analytics setting names must start with "spark" in order to not be ignored by Spark,
    //       and must not start with "spark.cassandra" so as to not conflict with Spark Cassandra Connector
    //       which will throw a configuration exception for each setting with that prefix it does not recognize
    public static final String SETTING_PREFIX = "spark.cassandra_analytics.";

    public static final String HTTP_MAX_CONNECTIONS                    = SETTING_PREFIX + "request.max_connections";
    public static final String HTTP_RESPONSE_TIMEOUT                   = SETTING_PREFIX + "request.response_timeout";
    public static final String HTTP_CONNECTION_TIMEOUT                 = SETTING_PREFIX + "request.connection_timeout";
    public static final String SIDECAR_PORT                            = SETTING_PREFIX + "ports.sidecar";
    public static final String SIDECAR_REQUEST_RETRIES                 = SETTING_PREFIX + "sidecar.request.retries";
    public static final String SIDECAR_REQUEST_RETRY_DELAY_SECONDS     = SETTING_PREFIX + "sidecar.request.retries.delay.seconds";
    public static final String SIDECAR_REQUEST_MAX_RETRY_DELAY_SECONDS = SETTING_PREFIX + "sidecar.request.retries.max.delay.seconds";
    public static final String SKIP_CLEAN                              = SETTING_PREFIX + "job.skip_clean";
    public static final String USE_OPENSSL                             = SETTING_PREFIX + "use_openssl";
    public static final String RING_RETRY_COUNT                        = SETTING_PREFIX + "ring_retry_count";

    public final Set<? extends SidecarInstance> sidecarInstances;
    public final String keyspace;
    public final String table;
    public final ConsistencyLevel.CL consistencyLevel;
    public final String localDC;
    public final Integer numberSplits;
    public final RowBufferMode rowBufferMode;
    public final Integer sstableDataSizeInMB;
    public final Integer sstableBatchSize;
    public final int commitBatchSize;
    public final boolean skipExtendedVerify;
    public final WriteMode writeMode;
    protected final String keystorePassword;
    protected final String keystorePath;
    protected final String keystoreBase64Encoded;
    protected final String keystoreType;
    protected final String truststorePassword;
    protected final String truststorePath;
    protected final String truststoreBase64Encoded;
    protected final String truststoreType;
    protected final String ttl;
    protected final String timestamp;
    protected final SparkConf conf;
    public final boolean validateSSTables;
    public final int commitThreadsPerInstance;
    protected final int sidecarPort;
    protected boolean useOpenSsl;
    protected int ringRetryCount;

    public BulkSparkConf(SparkConf conf, Map<String, String> options)
    {
        this.conf = conf;
        this.sidecarPort = MapUtils.getInt(options, WriterOptions.SIDECAR_PORT.name(), getInt(SIDECAR_PORT, DEFAULT_SIDECAR_PORT), "sidecar port");
        this.sidecarInstances = buildSidecarInstances(options, sidecarPort);
        this.keyspace = MapUtils.getOrThrow(options, WriterOptions.KEYSPACE.name());
        this.table = MapUtils.getOrThrow(options, WriterOptions.TABLE.name());
        this.validateSSTables = MapUtils.getBoolean(options, WriterOptions.VALIDATE_SSTABLES.name(), true, "validate SSTables");
        this.skipExtendedVerify = MapUtils.getBoolean(options, WriterOptions.SKIP_EXTENDED_VERIFY.name(), true,
                                                      "skip extended verification of SSTables by Cassandra");
        this.consistencyLevel = ConsistencyLevel.CL.valueOf(MapUtils.getOrDefault(options, WriterOptions.BULK_WRITER_CL.name(), "EACH_QUORUM"));
        this.localDC = MapUtils.getOrDefault(options, WriterOptions.LOCAL_DC.name(), null);
        this.numberSplits = MapUtils.getInt(options, WriterOptions.NUMBER_SPLITS.name(), DEFAULT_NUM_SPLITS, "number of splits");
        this.rowBufferMode = MapUtils.getEnumOption(options, WriterOptions.ROW_BUFFER_MODE.name(), DEFAULT_ROW_BUFFER_MODE, "row buffering mode");
        this.sstableDataSizeInMB = MapUtils.getInt(options, WriterOptions.SSTABLE_DATA_SIZE_IN_MB.name(), 160, "sstable data size in MB");
        this.sstableBatchSize = MapUtils.getInt(options, WriterOptions.BATCH_SIZE.name(), 1_000_000, "sstable batch size");
        this.commitBatchSize = MapUtils.getInt(options, WriterOptions.COMMIT_BATCH_SIZE.name(), DEFAULT_COMMIT_BATCH_SIZE, "commit batch size");
        this.commitThreadsPerInstance = MapUtils.getInt(options, WriterOptions.COMMIT_THREADS_PER_INSTANCE.name(), 2, "commit threads per instance");
        this.keystorePassword = MapUtils.getOrDefault(options, WriterOptions.KEYSTORE_PASSWORD.name(), null);
        this.keystorePath = MapUtils.getOrDefault(options, WriterOptions.KEYSTORE_PATH.name(), null);
        this.keystoreBase64Encoded = MapUtils.getOrDefault(options, WriterOptions.KEYSTORE_BASE64_ENCODED.name(), null);
        this.keystoreType = MapUtils.getOrDefault(options, WriterOptions.KEYSTORE_TYPE.name(), "PKCS12");
        this.truststorePassword = MapUtils.getOrDefault(options, WriterOptions.TRUSTSTORE_PASSWORD.name(), null);
        this.truststorePath = MapUtils.getOrDefault(options, WriterOptions.TRUSTSTORE_PATH.name(), null);
        this.truststoreBase64Encoded = MapUtils.getOrDefault(options, WriterOptions.TRUSTSTORE_BASE64_ENCODED.name(), null);
        this.truststoreType = MapUtils.getOrDefault(options, WriterOptions.TRUSTSTORE_TYPE.name(), null);
        this.writeMode = MapUtils.getEnumOption(options, WriterOptions.WRITE_MODE.name(), WriteMode.INSERT, "write mode");
        // For backwards-compatibility with port settings, use writer option if available,
        // else fall back to props, and then default if neither specified
        this.useOpenSsl = getBoolean(USE_OPENSSL, true);
        this.ringRetryCount = getInt(RING_RETRY_COUNT, DEFAULT_RING_RETRY_COUNT);
        this.ttl = MapUtils.getOrDefault(options, WriterOptions.TTL.name(), null);
        this.timestamp = MapUtils.getOrDefault(options, WriterOptions.TIMESTAMP.name(), null);
        validateEnvironment();
    }

    protected Set<? extends SidecarInstance> buildSidecarInstances(Map<String, String> options, int sidecarPort)
    {
        String sidecarInstances = MapUtils.getOrThrow(options, WriterOptions.SIDECAR_INSTANCES.name(), "sidecar_instances");
        return Arrays.stream(sidecarInstances.split(","))
                     .map(hostname -> new SidecarInstanceImpl(hostname, sidecarPort))
                     .collect(Collectors.toSet());
    }

    protected void validateEnvironment() throws RuntimeException
    {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(table);
        Preconditions.checkArgument(getHttpResponseTimeoutMs() > 0, HTTP_RESPONSE_TIMEOUT + " must be > 0");
        validateSslConfiguration();
        validateTableWriterSettings();
        CassandraBridgeFactory.validateBridges();
    }

    protected void validateTableWriterSettings()
    {
        boolean batchSizeIsZero = sstableBatchSize == 0;

        if (rowBufferMode == RowBufferMode.UNBUFFERED)
        {
            Preconditions.checkArgument(!batchSizeIsZero,
                                        "If writing in sorted order (ROW_BUFFER_MODE is UNBUFFERED) then BATCH_SIZE "
                                        + "should be non zero, but it was set to 0 in writer options");
        }
        else if (!batchSizeIsZero && sstableBatchSize != DEFAULT_BATCH_SIZE_IN_ROWS)
        {
            LOGGER.warn("BATCH_SIZE is set to a non-zero, non-default value ({}) but ROW_BUFFER_MODE is set to BUFFERED."
                        + " Ignoring BATCH_SIZE.", sstableBatchSize);
        }
    }

    /**
     * Validates the SSL configuration present and throws an exception if it is incorrect
     *
     * @throws NullPointerException if the mTLS KeyStore password is provided,
     *                              but both file path and base64 string are missing;
     *                              or if either mTLS TrustStore file path or base64 string is provided,
     *                              but the password is missing
     * @throws IllegalArgumentException if the mTLS TrustStore password is provided,
     *                                  but both file path and base64 string are missing
     */
    public void validateSslConfiguration()
    {
        if (getKeyStorePassword() != null)
        {
            // If the mTLS keystore password is provided, we validate that the path or a base64 keystore is provided
            if (getKeyStorePath() == null && getKeystoreBase64Encoded() == null)
            {
                throw new NullPointerException("Keystore password was set. "
                                             + "But both keystore path and base64 encoded string are not set. "
                                             + "Please either set option " + WriterOptions.KEYSTORE_PATH
                                             + " or option " + WriterOptions.KEYSTORE_BASE64_ENCODED);
            }
        }

        // Check to make sure if either trust store password or trust store path are specified, both are specified
        if (getConfiguredTrustStorePassword() != null)
        {
            Preconditions.checkArgument(getTruststoreBase64Encoded() != null || getTrustStorePath() != null,
                                        "Trust Store password was provided, but both truststore path and base64 encoded string are missing. "
                                      + "Please provide either option " + WriterOptions.TRUSTSTORE_PATH
                                      + " or option " + WriterOptions.TRUSTSTORE_BASE64_ENCODED);
        }
        else
        {
            if (getTrustStorePath() != null || getTruststoreBase64Encoded() != null)
            {
                Preconditions.checkNotNull(getTruststoreBase64Encoded(),
                                           "Trust Store Path was provided, but password is missing. "
                                         + "Please provide option " + WriterOptions.TRUSTSTORE_PASSWORD);
                Preconditions.checkNotNull(getTrustStorePath(),
                                           "Trust Store Base64 encoded was provided, but password is missing."
                                         + "Please provide option " + WriterOptions.TRUSTSTORE_PASSWORD);
            }
        }
    }

    public int getSidecarPort()
    {
        return sidecarPort;
    }

    protected String getTrustStorePath()
    {
        return truststorePath;
    }

    protected TTLOption getTTLOptions()
    {
        return TTLOption.from(ttl);
    }

    protected TimestampOption getTimestampOptions()
    {
        return TimestampOption.from(timestamp);
    }

    protected String getTruststoreBase64Encoded()
    {
        return truststoreBase64Encoded;
    }

    public String getTrustStoreTypeOrDefault()
    {
        return truststoreType != null ? truststoreType : "PKCS12";
    }

    protected String getKeyStorePath()
    {
        return keystorePath;
    }

    protected String getKeystoreBase64Encoded()
    {
        return keystoreBase64Encoded;
    }

    public InputStream getKeyStore()
    {
        return getKeyStorePath() != null
               ? getKeyStoreFromPath(getKeyStorePath())
               : getKeyStoreFromBase64EncodedString(getKeystoreBase64Encoded());
    }

    @Nullable
    public InputStream getTrustStore()
    {
        return getTrustStorePath() != null
               ? getKeyStoreFromPath(getTrustStorePath())
               : getKeyStoreFromBase64EncodedString(getTruststoreBase64Encoded());
    }

    protected InputStream getKeyStoreFromPath(String keyStorePath)
    {
        if (keyStorePath != null)
        {
            try
            {
                return new FileInputStream(keyStorePath);
            }
            catch (FileNotFoundException exception)
            {
                throw new RuntimeException("Could not load keystore at path '" + keyStorePath + "'", exception);
            }
        }
        return null;
    }

    protected InputStream getKeyStoreFromBase64EncodedString(String keyStoreBase64Encoded)
    {
        if (keyStoreBase64Encoded != null)
        {
            return new ByteArrayInputStream(Base64.getDecoder().decode(keyStoreBase64Encoded));
        }
        return null;
    }

    public String getTrustStorePasswordOrDefault()
    {
        return truststorePassword != null ? truststorePassword : "password";
    }

    public String getKeyStoreTypeOrDefault()
    {
        return keystoreType != null ? keystoreType : "PKCS12";
    }

    public String getKeyStorePassword()
    {
        return keystorePassword;
    }

    public String getConfiguredTrustStorePassword()
    {
        return truststorePassword;
    }

    public String getConfiguredKeyStorePassword()
    {
        return keystorePassword;
    }

    public int getSidecarRequestRetries()
    {
        return getInt(SIDECAR_REQUEST_RETRIES, DEFAULT_SIDECAR_REQUEST_RETRIES);
    }

    public long getSidecarRequestRetryDelayInSeconds()
    {
        return getLong(SIDECAR_REQUEST_RETRY_DELAY_SECONDS, DEFAULT_SIDECAR_REQUEST_RETRY_DELAY_SECONDS);
    }

    public long getSidecarRequestMaxRetryDelayInSeconds()
    {
        return getLong(SIDECAR_REQUEST_MAX_RETRY_DELAY_SECONDS, DEFAULT_SIDECAR_REQUEST_MAX_RETRY_DELAY_SECONDS);
    }

    public int getHttpConnectionTimeoutMs()
    {
        return getInt(HTTP_CONNECTION_TIMEOUT, DEFAULT_HTTP_CONNECTION_TIMEOUT);
    }

    public int getHttpResponseTimeoutMs()
    {
        return getInt(HTTP_RESPONSE_TIMEOUT, DEFAULT_HTTP_RESPONSE_TIMEOUT);
    }

    public int getMaxHttpConnections()
    {
        return getInt(HTTP_MAX_CONNECTIONS, DEFAULT_HTTP_MAX_CONNECTIONS);
    }

    public boolean getSkipClean()
    {
        return getBoolean(SKIP_CLEAN, false);
    }

    public Integer getCores()
    {
        int coresPerExecutor = conf.getInt("spark.executor.cores", 1);
        int numExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", conf.getInt("spark.executor.instances", 1));
        return coresPerExecutor * numExecutors;
    }

    protected int getInt(String settingName, int defaultValue)
    {
        String finalSetting = getSettingNameOrDeprecatedName(settingName);
        return conf.getInt(finalSetting, defaultValue);
    }

    protected long getLong(String settingName, long defaultValue)
    {
        String finalSetting = getSettingNameOrDeprecatedName(settingName);
        return conf.getLong(finalSetting, defaultValue);
    }

    protected boolean getBoolean(String settingName, boolean defaultValue)
    {
        String finalSetting = getSettingNameOrDeprecatedName(settingName);
        return conf.getBoolean(finalSetting, defaultValue);
    }

    protected String getSettingNameOrDeprecatedName(String settingName)
    {
        if (!conf.contains(settingName))
        {
            String settingSuffix = settingName.startsWith(SETTING_PREFIX)
                    ? settingName.substring(SETTING_PREFIX.length())
                    : settingName;
            for (String settingPrefix : getDeprecatedSettingPrefixes())
            {
                String deprecatedSetting = settingPrefix + settingSuffix;
                if (conf.contains(deprecatedSetting))
                {
                    LOGGER.warn("Found deprecated setting '{}'. Please use {} in the future.",
                                deprecatedSetting, settingName);
                    return deprecatedSetting;
                }
            }
        }
        return settingName;
    }

    @NotNull
    protected List<String> getDeprecatedSettingPrefixes()
    {
        return Collections.emptyList();
    }

    /**
     * The SBW utilizes Cassandra libraries to generate SSTables. Under JDK11, this library needs additional JVM options
     * to be set for the executors for some backward-compatibility reasons. This method will add the appropriate
     * JVM options. Additionally, we set up the SBW KryoRegistrator here rather than requiring the end-user
     * to call 2 static methods to set up the appropriate settings for the job.
     *
     * @param conf               the Spark Configuration to set up
     * @param addKryoRegistrator passs true if your application hasn't separately added
     *                           the bulk-specific Kryo registrator, false if you have set it up
     *                           separately (see the usage docs for more details)
     */
    public static void setupSparkConf(SparkConf conf, boolean addKryoRegistrator)
    {
        String previousOptions = conf.get("spark.executor.extraJavaOptions", "");
        if (BuildInfo.isAtLeastJava11(BuildInfo.javaSpecificationVersion()))
        {
            conf.set("spark.executor.extraJavaOptions", previousOptions + JDK11_OPTIONS);
        }

        if (addKryoRegistrator)
        {
            // Use `SbwKryoRegistrator.setupKryoRegistrator(conf);` to add the Spark Bulk Writer's custom
            // KryoRegistrator. This needs to happen before the SparkSession is built in order for it to work properly.
            // This utility method will add the necessary configuration, but it may be overwritten later if your own
            // code resets `spark.kryo.registrator` after this is called.
            SbwKryoRegistrator.setupKryoRegistrator(conf);
        }
    }

    protected SparkConf getConf()
    {
        return conf;
    }

    public boolean getUseOpenSsl()
    {
        return useOpenSsl;
    }

    public int getRingRetryCount()
    {
        return ringRetryCount;
    }

    public boolean hasKeystoreAndKeystorePassword()
    {
        return keystorePassword != null && (keystorePath != null || keystoreBase64Encoded != null);
    }

    public boolean hasTruststoreAndTruststorePassword()
    {
        return truststorePassword != null && (truststorePath != null || truststoreBase64Encoded != null);
    }
}
