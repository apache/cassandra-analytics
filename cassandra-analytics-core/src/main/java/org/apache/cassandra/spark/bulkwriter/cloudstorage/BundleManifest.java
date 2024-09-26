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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.cassandra.bridge.SSTableSummary;

/**
 * Manifest of all SSTables in the bundle
 * It is a variant of {@link HashMap}, for the convenience of json serialization
 */
public class BundleManifest extends HashMap<String, BundleManifest.Entry>
{
    public static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final long serialVersionUID = 6593130321276240266L;

    public void addEntry(Entry manifestEntry)
    {
        super.put(manifestEntry.key, manifestEntry);
    }

    public void persistTo(Path filePath) throws IOException
    {
        Files.createFile(filePath);
        OBJECT_WRITER.writeValue(filePath.toFile(), this);
    }

    /**
     * Manifest of a single SSTable
     * componentsChecksum include checksums of individual SSTable components
     * startToken and endToken represents the token range of the SSTable
     */
    public static class Entry
    {
        // uniquely identify a manifest entry
        private final String key;
        private final Map<String, String> componentsChecksum;
        private final BigInteger startToken;
        private final BigInteger endToken;

        @VisibleForTesting
        Entry(String key, BigInteger startToken, BigInteger endToken)
        {
            this.key = key;
            this.startToken = startToken;
            this.endToken = endToken;
            this.componentsChecksum = new HashMap<>();
        }

        public Entry(SSTableSummary summary)
        {
            this.key = summary.sstableId;
            this.startToken = summary.firstToken;
            this.endToken = summary.lastToken;
            this.componentsChecksum = new HashMap<>();
        }

        public void addComponentChecksum(String component, String checksum)
        {
            componentsChecksum.put(component, checksum);
        }

        @JsonProperty("components_checksum")
        public Map<String, String> componentsChecksum()
        {
            return componentsChecksum;
        }

        @JsonProperty("start_token")
        public BigInteger startToken()
        {
            return startToken;
        }

        @JsonProperty("end_token")
        public BigInteger endToken()
        {
            return endToken;
        }
    }
}
