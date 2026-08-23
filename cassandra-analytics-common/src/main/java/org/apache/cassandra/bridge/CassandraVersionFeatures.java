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

package org.apache.cassandra.bridge;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CassandraVersionFeatures implements Comparable<CassandraVersionFeatures>
{
    private static final Pattern VERSION_PATTERN_3        = Pattern.compile("(?:.+-)?([0-9]+)\\.([0-9]+)\\.([0-9]+)([a-zA-Z0-9-]*)");
    private static final Pattern VERSION_PATTERN_4        = Pattern.compile("(?:.+-)?([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)[a-zA-Z0-9-]*");
    // A major version carries no patch number until its first release: cassandra-4.0-SNAPSHOT, cassandra-6.0-alpha2,
    // 6.0-alpha2-SNAPSHOT for a node built from the pre-release branch, and a bare 6.0 where a caller names the
    // major version alone. Group 3 is the suffix, absent for a bare version
    private static final Pattern VERSION_PATTERN_NO_PATCH  = Pattern.compile("(?:.+-)?([0-9]+)\\.([0-9]+)(?:-([a-zA-Z][a-zA-Z0-9]*))?(?:-SNAPSHOT)?$");
    // Ordered most specific first; matchVersion returns the first that matches
    private static final List<Pattern> VERSION_PATTERNS = ImmutableList.of(VERSION_PATTERN_4,
                                                                          VERSION_PATTERN_3,
                                                                          VERSION_PATTERN_NO_PATCH);

    protected final int majorVersion;
    protected final int minorVersion;
    protected final String suffix;

    private final String rawVersionString;

    @VisibleForTesting
    public CassandraVersionFeatures(int majorVersion, int minorVersion, @Nullable String suffix)
    {
        this(majorVersion, minorVersion, suffix, null);
    }

    public CassandraVersionFeatures(int majorVersion, int minorVersion, @Nullable String suffix, @Nullable String rawVersionString)
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.suffix = suffix;
        this.rawVersionString = rawVersionString;
    }

    /**
     * Given a cassandra version string (e.g. cassandra-2.0.14-v3) parse it into a CassandraVersionFeatures object
     *
     * @param cassandraVersion the string representing the cassandra version
     * @return an instance representing the parsed values from the version string
     */
    public static CassandraVersionFeatures cassandraVersionFeaturesFromCassandraVersion(@NotNull String cassandraVersion)
    {
        String versionCode = getCassandraVersionCode(cassandraVersion);
        String minorVersionCode = getCassandraMinorVersionCode(cassandraVersion);
        String versionSuffix = getCassandraVersionSuffix(cassandraVersion);

        return new CassandraVersionFeatures(Integer.parseInt(versionCode), Integer.parseInt(minorVersionCode), versionSuffix, cassandraVersion);
    }

    // E.g if cassandra version = cassandra-1.2.11-v1, we return 12;
    //  or if cassandra version = cassandra-4.0-SNAPSHOT, we return 40
    private static String getCassandraVersionCode(String cassandraVersion)
    {
        Matcher matcher = matchVersion(cassandraVersion);

        return matcher.group(1) + matcher.group(2);
    }

    // E.g if cassandra version = cassandra-1.2.11-v1, we return 11;
    //  or if cassandra version = cassandra-4.0-SNAPSHOT, we return 0
    //  or if cassandra version = cassandra-6.0-alpha2, we return 0
    private static String getCassandraMinorVersionCode(String cassandraVersion)
    {
        Matcher matcher = matchVersion(cassandraVersion);
        if (!hasPatchNumber(matcher))
        {
            return "0";
        }

        return matcher.group(3);
    }

    // E.g if cassandra version = cassandra-1.2.11-v1, we return -v1;
    //  or if cassandra version = cassandra-1.2.11.2-tag, we return 2;
    //  or if cassandra version = cassandra-4.0-SNAPSHOT, we return SNAPSHOT
    //  or if cassandra version = cassandra-6.0-alpha2, we return alpha2
    //  or if cassandra version = 6.0, we return null
    private static String getCassandraVersionSuffix(String cassandraVersion)
    {
        Matcher matcher = matchVersion(cassandraVersion);
        if (!hasPatchNumber(matcher))
        {
            return matcher.group(3);
        }
        if (matchesSnapshot(matcher.group(4)))
        {
            return "SNAPSHOT";
        }

        return matcher.group(4);
    }

    // True when group 3 is the patch number and group 4 the suffix; false when group 3 is the suffix
    private static boolean hasPatchNumber(Matcher matcher)
    {
        return matcher.groupCount() > 3;
    }

    private static boolean matchesSnapshot(String snapshot)
    {
        return "SNAPSHOT".equals(snapshot) || "-SNAPSHOT".equals(snapshot);
    }

    /**
     * Returns a matched matcher using the first of {@link #VERSION_PATTERNS} that matches; throws if none match
     */
    private static Matcher matchVersion(String cassandraVersion)
    {
        for (Pattern pattern : VERSION_PATTERNS)
        {
            Matcher matcher = pattern.matcher(cassandraVersion);
            if (matcher.find())
            {
                return matcher;
            }
        }
        throw new RuntimeException("cassandraVersion does not match any version pattern, patterns=" + VERSION_PATTERNS
                                                                              + ", version=" + cassandraVersion);
    }

    public int getMajorVersion()
    {
        return majorVersion;
    }

    public int getMinorVersion()
    {
        return minorVersion;
    }

    public String getSuffix()
    {
        return suffix;
    }

    public String getRawVersionString()
    {
        return rawVersionString;
    }

    @Override
    public int compareTo(@NotNull CassandraVersionFeatures that)
    {
        int difference = this.getMajorVersion() - that.getMajorVersion();
        if (difference != 0)
        {
            return difference;
        }

        difference = this.getMinorVersion() - that.getMinorVersion();
        if (difference != 0)
        {
            return difference;
        }

        // Try to treat suffix as micro version
        try
        {
            return Integer.parseInt(getSuffix()) - Integer.parseInt(that.getSuffix());
        }
        catch (NumberFormatException exception)
        {
            // One with smallest suffix will be smallest
            int thisLength = getSuffix() != null ? getSuffix().length() : 0;
            int thatLength = that.getSuffix() != null ? that.getSuffix().length() : 0;
            return thisLength - thatLength;
        }
    }

    @Override
    public boolean equals(@Nullable Object other)
    {
        if (other == null || !(other instanceof CassandraVersionFeatures))
        {
            return false;
        }

        CassandraVersionFeatures that = (CassandraVersionFeatures) other;
        return Objects.equals(this.majorVersion, that.majorVersion)
            && Objects.equals(this.minorVersion, that.minorVersion)
            && Objects.equals(this.suffix, that.suffix);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(majorVersion, minorVersion, suffix);
    }

    @Override
    @NotNull
    public String toString()
    {
        return String.format("CassandraVersionFeatures{majorVersion=%d, minorVersion=%d, suffix='%s'}",
                             majorVersion, minorVersion, suffix);
    }
}
