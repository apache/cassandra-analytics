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

import org.apache.cassandra.spark.common.DataObjectBuilder;

/**
 * Storage object of the uploaded bundle, including object key and checksum
 */
public class BundleStorageObject
{
    public final String storageObjectKey;
    public final String storageObjectChecksum;
    public final Bundle bundle;

    static Builder builder()
    {
        return new Builder();
    }

    protected BundleStorageObject(Builder builder)
    {
        this.storageObjectChecksum = builder.storageObjectChecksum;
        this.storageObjectKey = builder.storageObjectKey;
        this.bundle = builder.bundle;
    }

    @Override
    public String toString()
    {
        return "Bundle{manifest: " + bundle
               + ", storageObjectKey: " + storageObjectKey
               + ", storageObjectChecksum: " + storageObjectChecksum + '}';
    }

    static class Builder implements DataObjectBuilder<Builder, BundleStorageObject>
    {
        private String storageObjectChecksum;
        private String storageObjectKey;
        private Bundle bundle;

        public Builder storageObjectChecksum(String bundleChecksum)
        {
            return with(b -> b.storageObjectChecksum = bundleChecksum);
        }

        public Builder storageObjectKey(String storageObjectKey)
        {
            return with(b -> b.storageObjectKey = storageObjectKey);
        }

        public Builder bundle(Bundle bundle)
        {
            return with(b -> b.bundle = bundle);
        }

        public BundleStorageObject build()
        {
            return new BundleStorageObject(this);
        }

        public Builder self()
        {
            return this;
        }
    }
}
