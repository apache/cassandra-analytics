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

package org.apache.cassandra.spark.data.backup;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.spark.data.S3ClientConfig;

/**
 * Configuration bundle passed to {@link BackupReaderFactory#create(BackupReaderConfig)}. Holds
 * the inputs an implementation needs to instantiate a {@link BackupReader} (S3 client config and
 * arbitrary string-keyed custom properties for vendor-specific knobs).
 *
 * <p>Task metrics sinks are supplied on individual {@link BackupReader} read calls.
 */
public final class BackupReaderConfig implements Serializable
{
    private static final long serialVersionUID = 2L;

    private final S3ClientConfig s3Config;
    private final Map<String, String> customProperties;

    private BackupReaderConfig(S3ClientConfig s3Config, Map<String, String> customProperties)
    {
        this.s3Config = s3Config;
        this.customProperties = customProperties != null ? customProperties : Collections.emptyMap();
    }

    /** Convenience factory: no custom properties. */
    public static BackupReaderConfig of(S3ClientConfig s3Config)
    {
        return new BackupReaderConfig(s3Config, Collections.emptyMap());
    }

    /** Factory with custom properties pre-populated. */
    public static BackupReaderConfig of(S3ClientConfig s3Config, Map<String, String> customProperties)
    {
        return new BackupReaderConfig(s3Config, customProperties);
    }

    public S3ClientConfig s3Config()
    {
        return s3Config;
    }

    public Map<String, String> customProperties()
    {
        return customProperties;
    }
}
