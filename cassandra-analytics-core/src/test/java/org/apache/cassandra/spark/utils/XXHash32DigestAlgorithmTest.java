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
import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.XXHash32Digest;

import static org.apache.cassandra.spark.utils.ResourceUtils.writeResourceToPath;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link XXHash32DigestAlgorithm}
 */
class XXHash32DigestAlgorithmTest
{
    @TempDir
    private Path tempPath;

    // To generate test files I used:
    // $ base64 -i /dev/urandom | head -c 1048576 > file1.txt
    // $ base64 -i /dev/urandom | head -c 524288 > file2.txt
    // $ base64 -i /dev/urandom | head -c 131072 > file3.txt
    // To calculate xxhash I installed xxhash (brew install xxhash):
    // $ xxh32sum file1.txt # -> d76a44a5
    // $ xxh32sum file2.txt # -> ef976cbe
    // $ xxh32sum file3.txt # -> 08321e1e

    @ParameterizedTest(name = "{index} fileName={0} expectedDigest={1}")
    @CsvSource({
    "file1.txt,d76a44a5",
    "file2.txt,ef976cbe",
    "file3.txt,8321e1e",
    })
    void testXXHash32Provider(String fileName, String expectedXXHash32) throws IOException
    {
        ClassLoader classLoader = MD5DigestAlgorithmTest.class.getClassLoader();
        Path path = writeResourceToPath(classLoader, tempPath, "digest/" + fileName);
        assertThat(path).exists();

        Digest digest = new XXHash32DigestAlgorithm().calculateFileDigest(path);
        assertThat(digest).isInstanceOf(XXHash32Digest.class);

        XXHash32Digest xxHash32Digest = (XXHash32Digest) digest;
        assertThat(xxHash32Digest.value()).isEqualTo(expectedXXHash32);
        assertThat(xxHash32Digest.seedHex()).isEqualTo("0");
    }
}
