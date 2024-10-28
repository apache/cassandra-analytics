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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.StorageClientConfig;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf.SimpleClusterConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.util.SbwKryoRegistrator;
import org.apache.cassandra.spark.common.SidecarInstanceFactory;
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
    public static final int DEFAULT_HTTP_MAX_CONNECTIONS = 25;
    public static final int DEFAULT_SIDECAR_PORT = 9043;
    public static final int DEFAULT_SIDECAR_REQUEST_RETRIES = 10;
    public static final long DEFAULT_SIDECAR_REQUEST_RETRY_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(1L);
    public static final long DEFAULT_SIDECAR_REQUEST_MAX_RETRY_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(60L);
    public static final int DEFAULT_SIDECAR_REQUEST_TIMEOUT_SECONDS = 300;
    public static final int DEFAULT_COMMIT_BATCH_SIZE = 10_000;
    public static final int DEFAULT_RING_RETRY_COUNT = 3;
    public static final int DEFAULT_SSTABLE_DATA_SIZE_IN_MIB = 160;
    public static final long DEFAULT_STORAGE_CLIENT_KEEP_ALIVE_SECONDS = 60;
    public static final int DEFAULT_STORAGE_CLIENT_CONCURRENCY = Runtime.getRuntime().availableProcessors() * 2;
    public static final int DEFAULT_STORAGE_CLIENT_MAX_CHUNK_SIZE_IN_BYTES = 100 * 1024 * 1024; // 100 MiB
    private static final long DEFAULT_MAX_SIZE_PER_SSTABLE_BUNDLE_IN_BYTES_S3_TRANSPORT = 5L * 1024 * 1024 * 1024;

    // NOTE: All Cassandra Analytics setting names must start with "spark" in order to not be ignored by Spark,
    //       and must not start with "spark.cassandra" so as to not conflict with Spark Cassandra Connector
    //       which will throw a configuration exception for each setting with that prefix it does not recognize
    public static final String SETTING_PREFIX = "spark.cassandra_analytics.";

    public static final String HTTP_MAX_CONNECTIONS                    = SETTING_PREFIX + "request.max_connections";
    public static final String HTTP_RESPONSE_TIMEOUT                   = SETTING_PREFIX + "request.response_timeout";
    public static final String HTTP_CONNECTION_TIMEOUT                 = SETTING_PREFIX + "request.connection_timeout";
    public static final String SIDECAR_PORT                            = SETTING_PREFIX + "ports.sidecar";
    public static final String SIDECAR_REQUEST_RETRIES                 = SETTING_PREFIX + "sidecar.request.retries";
    public static final String SIDECAR_REQUEST_RETRY_DELAY_MILLIS      = SETTING_PREFIX + "sidecar.request.retries.delay.milliseconds";
    public static final String SIDECAR_REQUEST_MAX_RETRY_DELAY_MILLIS  = SETTING_PREFIX + "sidecar.request.retries.max.delay.milliseconds";
    public static final String SIDECAR_REQUEST_TIMEOUT_SECONDS         = SETTING_PREFIX + "sidecar.request.timeout.seconds";
    public static final String SKIP_CLEAN                              = SETTING_PREFIX + "job.skip_clean";
    public static final String USE_OPENSSL                             = SETTING_PREFIX + "use_openssl";
    // defines the max number of consecutive retries allowed in the ring monitor
    public static final String RING_RETRY_COUNT                        = SETTING_PREFIX + "ring_retry_count";
    public static final String IMPORT_COORDINATOR_TIMEOUT_MULTIPLIER   = SETTING_PREFIX + "importCoordinatorTimeoutMultiplier";
    public static final int MINIMUM_JOB_KEEP_ALIVE_MINUTES             = 10;

    public final String keyspace;
    public final String table;
    public final ConsistencyLevel.CL consistencyLevel;
    public final String localDC;
    public final Integer numberSplits;
    public final Integer sstableDataSizeInMiB;
    public final int commitBatchSize;
    public final boolean skipExtendedVerify;
    public final WriteMode writeMode;
    public final int commitThreadsPerInstance;
    public final double importCoordinatorTimeoutMultiplier;
    public boolean quoteIdentifiers;
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
    protected final int effectiveSidecarPort;
    protected final int userProvidedSidecarPort;
    protected final DigestAlgorithmSupplier digestAlgorithmSupplier;
    protected final StorageClientConfig storageClientConfig;
    protected final DataTransportInfo dataTransportInfo;
    protected final int jobKeepAliveMinutes;
    protected final long jobTimeoutSeconds;
    // An optional unique identifier supplied by customer. The jobId is different from restoreJobId that is used internally.
    // The value is null when absent
    protected final String configuredJobId;
    protected boolean useOpenSsl;
    protected int ringRetryCount;
    // create sidecarInstances from sidecarContactPointsValue and effectiveSidecarPort
    private final String sidecarContactPointsValue; // It takes comma separated values
    private transient Set<SidecarInstance> sidecarContactPoints; // not serialized
    private final String coordinatedWriteConfJson;
    private transient CoordinatedWriteConf coordinatedWriteConf; // it is transient; deserialized from coordinatedWriteConfJson in executors

    public BulkSparkConf(SparkConf conf, Map<String, String> options)
    {
        this.conf = conf;
        Optional<Integer> sidecarPortFromOptions = MapUtils.getOptionalInt(options, WriterOptions.SIDECAR_PORT.name(), "sidecar port");
        this.userProvidedSidecarPort = sidecarPortFromOptions.isPresent() ? sidecarPortFromOptions.get() : getOptionalInt(SIDECAR_PORT).orElse(-1);
        this.effectiveSidecarPort = this.userProvidedSidecarPort == -1 ? DEFAULT_SIDECAR_PORT : this.userProvidedSidecarPort;
        this.sidecarContactPointsValue = resolveSidecarContactPoints(options);
        this.keyspace = MapUtils.getOrThrow(options, WriterOptions.KEYSPACE.name());
        this.table = MapUtils.getOrThrow(options, WriterOptions.TABLE.name());
        this.skipExtendedVerify = MapUtils.getBoolean(options, WriterOptions.SKIP_EXTENDED_VERIFY.name(), true,
                                                      "skip extended verification of SSTables by Cassandra");
        this.consistencyLevel = ConsistencyLevel.CL.valueOf(MapUtils.getOrDefault(options, WriterOptions.BULK_WRITER_CL.name(), "EACH_QUORUM"));
        String dc = MapUtils.getOrDefault(options, WriterOptions.LOCAL_DC.name(), null);
        if (!consistencyLevel.isLocal() && dc != null)
        {
            LOGGER.warn("localDc is present for non-local consistency level {} specified in writer options. Correcting localDc to null", consistencyLevel);
            dc = null;
        }
        this.localDC = dc;
        this.numberSplits = MapUtils.getInt(options, WriterOptions.NUMBER_SPLITS.name(), DEFAULT_NUM_SPLITS, "number of splits");
        this.sstableDataSizeInMiB = resolveSSTableDataSizeInMiB(options);
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
        this.digestAlgorithmSupplier = digestAlgorithmSupplierFromOptions(options);
        // For backwards-compatibility with port settings, use writer option if available,
        // else fall back to props, and then default if neither specified
        this.useOpenSsl = getBoolean(USE_OPENSSL, true);
        this.ringRetryCount = getInt(RING_RETRY_COUNT, DEFAULT_RING_RETRY_COUNT);
        this.importCoordinatorTimeoutMultiplier = getDouble(IMPORT_COORDINATOR_TIMEOUT_MULTIPLIER, 0.5);
        this.ttl = MapUtils.getOrDefault(options, WriterOptions.TTL.name(), null);
        this.timestamp = MapUtils.getOrDefault(options, WriterOptions.TIMESTAMP.name(), null);
        this.quoteIdentifiers = MapUtils.getBoolean(options, WriterOptions.QUOTE_IDENTIFIERS.name(), false, "quote identifiers");
        int storageClientConcurrency = MapUtils.getInt(options, WriterOptions.STORAGE_CLIENT_CONCURRENCY.name(),
                                                       DEFAULT_STORAGE_CLIENT_CONCURRENCY, "storage client concurrency");
        long storageClientKeepAliveSeconds = MapUtils.getLong(options, WriterOptions.STORAGE_CLIENT_THREAD_KEEP_ALIVE_SECONDS.name(),
                                                              DEFAULT_STORAGE_CLIENT_KEEP_ALIVE_SECONDS);
        int storageClientMaxChunkSizeInBytes = MapUtils.getInt(options, WriterOptions.STORAGE_CLIENT_MAX_CHUNK_SIZE_IN_BYTES.name(),
                                                               DEFAULT_STORAGE_CLIENT_MAX_CHUNK_SIZE_IN_BYTES);
        String storageClientHttpsProxy = MapUtils.getOrDefault(options, WriterOptions.STORAGE_CLIENT_HTTPS_PROXY.name(), null);
        String storageClientEndpointOverride = MapUtils.getOrDefault(options, WriterOptions.STORAGE_CLIENT_ENDPOINT_OVERRIDE.name(), null);
        long nioHttpClientConnectionAcquisitionTimeoutSeconds =
        MapUtils.getLong(options, WriterOptions.STORAGE_CLIENT_NIO_HTTP_CLIENT_CONNECTION_ACQUISITION_TIMEOUT_SECONDS.name(), 300);
        int nioHttpClientMaxConcurrency = MapUtils.getInt(options, WriterOptions.STORAGE_CLIENT_NIO_HTTP_CLIENT_MAX_CONCURRENCY.name(), 50);
        this.storageClientConfig = new StorageClientConfig(storageClientConcurrency,
                                                           storageClientKeepAliveSeconds,
                                                           storageClientMaxChunkSizeInBytes,
                                                           storageClientHttpsProxy,
                                                           storageClientEndpointOverride,
                                                           nioHttpClientConnectionAcquisitionTimeoutSeconds,
                                                           nioHttpClientMaxConcurrency);
        DataTransport dataTransport = MapUtils.getEnumOption(options, WriterOptions.DATA_TRANSPORT.name(), DataTransport.DIRECT, "Data Transport");
        long maxSizePerSSTableBundleInBytesS3Transport = MapUtils.getLong(options, WriterOptions.MAX_SIZE_PER_SSTABLE_BUNDLE_IN_BYTES_S3_TRANSPORT.name(),
                                                                          DEFAULT_MAX_SIZE_PER_SSTABLE_BUNDLE_IN_BYTES_S3_TRANSPORT);
        String transportExtensionClass = MapUtils.getOrDefault(options, WriterOptions.DATA_TRANSPORT_EXTENSION_CLASS.name(), null);
        this.dataTransportInfo = new DataTransportInfo(dataTransport, transportExtensionClass, maxSizePerSSTableBundleInBytesS3Transport);
        this.jobKeepAliveMinutes = MapUtils.getInt(options, WriterOptions.JOB_KEEP_ALIVE_MINUTES.name(), MINIMUM_JOB_KEEP_ALIVE_MINUTES);
        if (this.jobKeepAliveMinutes < MINIMUM_JOB_KEEP_ALIVE_MINUTES)
        {
            throw new IllegalArgumentException(String.format("Invalid value for the '%s' Bulk Writer option (%d). It cannot be less than the minimum %s",
                                                             WriterOptions.JOB_KEEP_ALIVE_MINUTES, jobKeepAliveMinutes, MINIMUM_JOB_KEEP_ALIVE_MINUTES));
        }
        this.jobTimeoutSeconds = MapUtils.getLong(options, WriterOptions.JOB_TIMEOUT_SECONDS.name(), -1L);
        this.configuredJobId = MapUtils.getOrDefault(options, WriterOptions.JOB_ID.name(), null);
        this.coordinatedWriteConfJson = MapUtils.getOrDefault(options, WriterOptions.COORDINATED_WRITE_CONFIG.name(), null);
        this.coordinatedWriteConf = buildCoordinatedWriteConf(dataTransportInfo.getTransport());
        validateEnvironment();
    }

    /**
     * Returns the supplier for the digest algorithm from the configured {@code options}.
     *
     * @param options a key-value map with options for the bulk write job
     * @return the configured {@link DigestAlgorithmSupplier}
     */
    @NotNull
    protected DigestAlgorithmSupplier digestAlgorithmSupplierFromOptions(Map<String, String> options)
    {
        return MapUtils.getEnumOption(options, WriterOptions.DIGEST.name(), DigestAlgorithms.XXHASH32, "digest type");
    }

    public static int resolveSSTableDataSizeInMiB(Map<String, String> options)
    {
        return MapUtils.resolveDeprecated(options, WriterOptions.SSTABLE_DATA_SIZE_IN_MIB.name(), WriterOptions.SSTABLE_DATA_SIZE_IN_MB.name(), option -> {
            if (option == null)
            {
                return DEFAULT_SSTABLE_DATA_SIZE_IN_MIB;
            }

            return MapUtils.getInt(options, option, DEFAULT_SSTABLE_DATA_SIZE_IN_MIB, "sstable data size in mebibytes");
        });
    }

    public static String resolveSidecarContactPoints(Map<String, String> options)
    {
        return MapUtils.resolveDeprecated(options, WriterOptions.SIDECAR_CONTACT_POINTS.name(), WriterOptions.SIDECAR_INSTANCES.name(), option -> {
            if (option == null)
            {
                return null;
            }

            return MapUtils.getOrDefault(options, option, null);
        });
    }

    protected Set<SidecarInstance> buildSidecarContactPoints()
    {
        String[] split = Objects.requireNonNull(sidecarContactPointsValue, "Unable to build sidecar instances from null value")
                                .split(",");
        return Arrays.stream(split)
                     .filter(StringUtils::isNotEmpty)
                     .map(hostname -> SidecarInstanceFactory.createFromString(hostname, effectiveSidecarPort))
                     .collect(Collectors.toSet());
    }

    Set<SidecarInstance> sidecarContactPoints()
    {
        if (sidecarContactPoints == null)
        {
            sidecarContactPoints = buildSidecarContactPoints();
        }
        return sidecarContactPoints;
    }

    public boolean isCoordinatedWriteConfigured()
    {
        return coordinatedWriteConf() != null;
    }

    public CoordinatedWriteConf coordinatedWriteConf()
    {
        if (coordinatedWriteConf == null)
        {
            coordinatedWriteConf = buildCoordinatedWriteConf(dataTransportInfo.getTransport());
        }

        return coordinatedWriteConf;
    }

    @Nullable
    protected CoordinatedWriteConf buildCoordinatedWriteConf(DataTransport dataTransport)
    {
        if (coordinatedWriteConfJson == null)
        {
            return null;
        }

        Preconditions.checkArgument(dataTransport == DataTransport.S3_COMPAT,
                                    "Coordinated write only supports " + DataTransport.S3_COMPAT);

        if (sidecarContactPointsValue != null)
        {
            LOGGER.warn("SIDECAR_CONTACT_POINTS or SIDECAR_INSTANCES are ignored on the presence of COORDINATED_WRITE_CONF");
        }

        if (userProvidedSidecarPort != -1)
        {
            LOGGER.warn("SIDECAR_PORT is ignored on the presence of COORDINATED_WRITE_CONF");
        }

        if (localDC != null)
        {
            LOGGER.warn("LOCAL_DC is ignored on the presence of COORDINATED_WRITE_CONF");
        }

        return CoordinatedWriteConf.create(coordinatedWriteConfJson, consistencyLevel, SimpleClusterConf.class);
    }

    protected void validateEnvironment() throws RuntimeException
    {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(table);
        Preconditions.checkArgument(getHttpResponseTimeoutMs() > 0, HTTP_RESPONSE_TIMEOUT + " must be > 0");
        validateSslConfiguration();
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

    public int getUserProvidedSidecarPort()
    {
        return userProvidedSidecarPort;
    }

    public int getEffectiveSidecarPort()
    {
        return effectiveSidecarPort;
    }

    protected String getTrustStorePath()
    {
        return truststorePath;
    }

    public TTLOption getTTLOptions()
    {
        return TTLOption.from(ttl);
    }

    public TimestampOption getTimestampOptions()
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

    public long getSidecarRequestRetryDelayMillis()
    {
        return getLong(SIDECAR_REQUEST_RETRY_DELAY_MILLIS, DEFAULT_SIDECAR_REQUEST_RETRY_DELAY_MILLIS);
    }

    public long getSidecarRequestMaxRetryDelayMillis()
    {
        return getLong(SIDECAR_REQUEST_MAX_RETRY_DELAY_MILLIS, DEFAULT_SIDECAR_REQUEST_MAX_RETRY_DELAY_MILLIS);
    }

    public int getSidecarRequestTimeoutSeconds()
    {
        return getInt(SIDECAR_REQUEST_TIMEOUT_SECONDS, DEFAULT_SIDECAR_REQUEST_TIMEOUT_SECONDS);
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

    public int getJobKeepAliveMinutes()
    {
        return jobKeepAliveMinutes;
    }

    public long getJobTimeoutSeconds()
    {
        return jobTimeoutSeconds;
    }

    protected double getDouble(String settingName, double defaultValue)
    {
        String finalSetting = getSettingNameOrDeprecatedName(settingName);
        return conf.getDouble(finalSetting, defaultValue);
    }

    protected int getInt(String settingName, int defaultValue)
    {
        String finalSetting = getSettingNameOrDeprecatedName(settingName);
        return conf.getInt(finalSetting, defaultValue);
    }

    protected Optional<Integer> getOptionalInt(String settingName)
    {
        String finalSetting = getSettingNameOrDeprecatedName(settingName);
        if (!conf.contains(finalSetting))
        {
            return Optional.empty();
        }
        try
        {
            return Optional.of(Integer.parseInt(conf.get(finalSetting)));
        }
        catch (NumberFormatException exception)
        {
            throw new IllegalArgumentException("Spark conf " + settingName + " is not set to a valid integer string.",
                                               exception);
        }
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

    public SparkConf getSparkConf()
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

    public StorageClientConfig getStorageClientConfig()
    {
        return storageClientConfig;
    }

    public DataTransportInfo getTransportInfo()
    {
        return dataTransportInfo;
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
