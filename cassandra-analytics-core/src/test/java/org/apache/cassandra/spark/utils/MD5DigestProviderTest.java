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
import java.nio.file.Paths;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.MD5Digest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MD5DigestProvider}
 */
class MD5DigestProviderTest
{
    // To generate test files I used:
    // $ base64 -i /dev/urandom | head -c 1048576 > file1.txt
    // $ base64 -i /dev/urandom | head -c 524288 > file2.txt
    // $ base64 -i /dev/urandom | head -c 131072 > file3.txt
    // To calculate MD5 I used:
    // $ md5 file1.txt # -> 56a4944588825e364880ff823bd2242d
    // $ md5 file2.txt # -> bc5a154ea567830ec9463f3225f93750
    // $ md5 file3.txt # -> 457012087b61492accb7b60e289e8e0d

    @ParameterizedTest(name = "{index} fileName={0} expectedMd5={1}")
    @CsvSource({
    "file1.txt,56a4944588825e364880ff823bd2242d",
    "file2.txt,bc5a154ea567830ec9463f3225f93750",
    "file3.txt,457012087b61492accb7b60e289e8e0d",
    })
    void testMD5Provider(String fileName, String expectedMd5) throws IOException
    {
        Path path = Paths.get("src", "test", "resources", "digest", fileName);
        assertThat(path).exists();

        Digest digest = new MD5DigestProvider().calculateFileDigest(path);
        assertThat(digest).isInstanceOf(MD5Digest.class);
        assertThat(digest.value()).isEqualTo(expectedMd5);
    }
}
