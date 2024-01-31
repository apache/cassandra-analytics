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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.XXHash32Digest;

/**
 * A {@link DigestAlgorithm} implementation that computes XXHash32 digests
 */
public class XXHash32DigestAlgorithm implements DigestAlgorithm
{
    private static final int KIB_512 = 512 * 1024;
    /**
     * A seed used to calculate the XXHash32 digest
     */
    private static final int SEED = 0;

    /**
     * Calculates the {@link org.apache.cassandra.spark.common.XXHash32Digest} for the given file in the {@code path}.
     *
     * @param path the path of the file
     * @return the calculated digest for the given {@code path}
     * @throws IOException when an error occurs while reading the file or calculating the digest
     */
    @Override
    public Digest calculateFileDigest(Path path) throws IOException
    {
        // might have shared hashers with ThreadLocal
        XXHashFactory factory = XXHashFactory.safeInstance();
        try (InputStream inputStream = Files.newInputStream(path);
             StreamingXXHash32 hasher = factory.newStreamingHash32(SEED))
        {
            int len;
            byte[] buffer = new byte[KIB_512];
            while ((len = inputStream.read(buffer)) != -1)
            {
                hasher.update(buffer, 0, len);
            }
            return new XXHash32Digest(Long.toHexString(hasher.getValue()), SEED);
        }
    }
}
