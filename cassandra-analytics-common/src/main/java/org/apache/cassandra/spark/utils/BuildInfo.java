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

package org.apache.cassandra.spark.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for serving build related information
 */
public final class BuildInfo
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BuildInfo.class);

    // Do not reorder! Build version constants must be initialized before user agent constants.
    public static final String APPLICATION_NAME = "cassandra-analytics";
    public static final String BUILD_VERSION = getBuildVersion();
    @SuppressWarnings("unused")  // Part of this library's API used by the consumers
    public static final String BUILD_VERSION_AND_REVISION = getBuildVersionAndRevision();
    public static final String READER_USER_AGENT = getUserAgent("reader");
    public static final String WRITER_USER_AGENT = getUserAgent("writer");
    public static final String WRITER_S3_USER_AGENT = getUserAgent("writer-s3");

    private BuildInfo()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private static String getUserAgent(String feature)
    {
        return String.format("%s/%s %s", APPLICATION_NAME, BUILD_VERSION, feature);
    }

    @VisibleForTesting
    static String getBuildVersion()
    {
        return getBuildInfo(properties -> properties.getProperty("build-version"));
    }

    /**
     * Determine whether the provided version is at least java 11
     *
     * @param version the java specification version
     * @return {@code true} if the java version is at least java 11, {@code false} otherwise
     */
    public static boolean isAtLeastJava11(String version)
    {
        if (version == null)
        {
            return false;
        }
        else if (version.contains("."))
        {
            return version.compareTo("11") >= 0;
        }
        else
        {
            return Integer.parseInt(version) >= 11;
        }
    }

    /**
     * @return the java specification version from the system properties, or {@code null} if a
     * {@link SecurityException} prevents us from reading the property
     */
    public static String javaSpecificationVersion()
    {
        String javaSpecificationVersionPropertyName = "java.specification.version";
        try
        {
            return System.getProperty(javaSpecificationVersionPropertyName);
        }
        catch (SecurityException exception)
        {
            LOGGER.error("Unable to determine java specification version from system property={}",
                         javaSpecificationVersionPropertyName, exception);
            return null;
        }
    }

    private static String getBuildVersionAndRevision()
    {
        return getBuildInfo(properties -> properties.getProperty("build-version") + "-" + properties.getProperty("build-rev"));
    }

    private static String getBuildInfo(Function<Properties, String> extractor)
    {
        try (InputStream in = BuildInfo.class.getResourceAsStream("/cassandra-analytics-build.properties"))
        {
            Properties properties = new Properties();
            properties.load(in);
            return extractor.apply(properties);
        }
        catch (Exception exception)
        {
            LOGGER.warn("Could not load the resource to get Cassandra Analytics build version", exception);
            return "unknown";
        }
    }
}
