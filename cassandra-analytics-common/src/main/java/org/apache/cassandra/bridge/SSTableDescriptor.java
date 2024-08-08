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

import java.util.Objects;

/**
 * Descriptor for each SSTable.
 *
 * (as of now, it is just a wrapper around the base filename of sstable; add more methods and properties when appropriate)
 */
public class SSTableDescriptor
{
    // base filename that is shared among all components of the same sstable
    public final String baseFilename;

    public SSTableDescriptor(String baseFilename)
    {
        this.baseFilename = baseFilename;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        SSTableDescriptor that = (SSTableDescriptor) o;
        return Objects.equals(baseFilename, that.baseFilename);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(baseFilename);
    }
}
