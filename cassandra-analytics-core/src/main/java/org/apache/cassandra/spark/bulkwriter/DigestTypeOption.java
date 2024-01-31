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

import org.apache.cassandra.spark.utils.DigestProvider;
import org.apache.cassandra.spark.utils.MD5DigestProvider;
import org.apache.cassandra.spark.utils.XXHash32DigestProvider;

/**
 * Represents the user-provided digest type configuration to be used to validate SSTable files during bulk writes
 */
public enum DigestTypeOption
{
    /**
     * Represents an MD5 digest type option. This option is supported for legacy reasons, but its use
     * is strongly discouraged.
     */
    MD5
    {
        @Override
        DigestProvider provider()
        {
            return new MD5DigestProvider();
        }
    },

    /**
     * Represents an xxhash32 digest type option
     */
    XXHASH32
    {
        @Override
        DigestProvider provider()
        {
            return new XXHash32DigestProvider();
        }
    };

    /**
     * @return the provider for the configured digest type
     */
    abstract DigestProvider provider();
}
