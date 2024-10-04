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

package org.apache.cassandra.spark.bulkwriter.blobupload;

import java.math.BigInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BundleNameGeneratorTest
{
    @Test
    void testNameGenerated()
    {
        String jobId = "ea3b3e6b-0d78-4913-89f2-15fcf98711d0";
        String sessionId = "1-9062a40b-41ae-40b0-8ba6-47f9bbec6cba";
        BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, sessionId);

        String expectedName = "b_" + jobId + '_' + sessionId + "_1_3";
        assertEquals(expectedName, nameGenerator.generate(BigInteger.valueOf(1L), BigInteger.valueOf(3L)));
        expectedName = "d_" + jobId + '_' + sessionId + "_3_6";
        assertEquals(expectedName, nameGenerator.generate(BigInteger.valueOf(3L), BigInteger.valueOf(6L)));
    }

    @Test
    void testAllStartCharsGenerated()
    {
        String jobId = "ea3b3e6b-0d78-4913-89f2-15fcf98711d0";
        char[] expectedResults = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

        String sessionId = "1-9062a40b-41ae-40b0-8ba6-47f9bbec6cba";
        BundleNameGenerator nameGenerator = new BundleNameGenerator(jobId, sessionId);

        // till 61 because of mod 62 results possible
        for (int i = 0; i < 62; i++)
        {
            assertEquals(expectedResults[i], nameGenerator.generate(BigInteger.valueOf(i), BigInteger.valueOf(i + 1)).charAt(0));
        }
    }

    @Test
    void testGenerateBundleNamePrefixChar()
    {
        IntStream.rangeClosed(Integer.MIN_VALUE, Integer.MAX_VALUE)
        .forEach(i -> {
            char prefix = BundleNameGenerator.generatePrefixChar(i);
            if (!(prefix >= 'a' && prefix <= 'z') &&
                !(prefix >= 'A' && prefix <= 'Z') &&
                !(prefix >= '0' && prefix <= '9'))
            {
                fail("Seed " + i + " produces invalid prefix " + prefix);
            }
        });
    }
}
