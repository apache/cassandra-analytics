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
import org.apache.cassandra.spark.common.MD5Digest;

import static org.apache.cassandra.spark.utils.ResourceUtils.writeResourceToPath;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MD5DigestAlgorithm}
 */
class MD5DigestAlgorithmTest
{
    @TempDir
    private Path tempPath;

    // To generate test files I used:
    // $ base64 -i /dev/urandom | head -c 1048576 > file1.txt
    // $ base64 -i /dev/urandom | head -c 524288 > file2.txt
    // $ base64 -i /dev/urandom | head -c 131072 > file3.txt
    // To calculate MD5 I used:
    // $ cat file1.txt | openssl dgst -md5 -binary | openssl enc -base64 # -> VqSURYiCXjZIgP+CO9IkLQ==
    // $ cat file2.txt | openssl dgst -md5 -binary | openssl enc -base64 # -> vFoVTqVngw7JRj8yJfk3UA==
    // $ cat file3.txt | openssl dgst -md5 -binary | openssl enc -base64 # -> RXASCHthSSrMt7YOKJ6ODQ==

    @ParameterizedTest(name = "{index} fileName={0} expectedMd5={1}")
    @CsvSource({
    "file1.txt,VqSURYiCXjZIgP+CO9IkLQ==",
    "file2.txt,vFoVTqVngw7JRj8yJfk3UA==",
    "file3.txt,RXASCHthSSrMt7YOKJ6ODQ==",
    })
    void testMD5Provider(String fileName, String expectedMd5) throws IOException
    {
        ClassLoader classLoader = MD5DigestAlgorithmTest.class.getClassLoader();
        Path path = writeResourceToPath(classLoader, tempPath, "digest/" + fileName);
        assertThat(path).exists();

        Digest digest = new MD5DigestAlgorithm().calculateFileDigest(path);
        assertThat(digest).isInstanceOf(MD5Digest.class);
        assertThat(digest.value()).isEqualTo(expectedMd5);
    }
}
