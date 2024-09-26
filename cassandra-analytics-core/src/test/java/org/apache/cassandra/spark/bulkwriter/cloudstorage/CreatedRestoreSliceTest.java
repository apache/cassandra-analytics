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

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;

import static org.assertj.core.api.Assertions.assertThat;

class CreatedRestoreSliceTest
{
    @Test
    void testEqualsAndHashcode()
    {
        CreateSliceRequestPayload req = new CreateSliceRequestPayload("id", 0, "bucket", "key", "checksum",
                                                                      BigInteger.ONE, BigInteger.TEN, 234L, 123L);
        CreatedRestoreSlice slice = new CreatedRestoreSlice(req);

        assertThat(slice.sliceRequestPayloadJson)
        .isEqualTo("{\"sliceId\":\"id\"," +
                   "\"bucketId\":0," +
                   "\"storageBucket\":\"bucket\"," +
                   "\"storageKey\":\"key\"," +
                   "\"sliceChecksum\":\"checksum\"," +
                   "\"startToken\":1," +
                   "\"endToken\":10," +
                   "\"sliceUncompressedSize\":234," +
                   "\"sliceCompressedSize\":123}");

        assertThat(slice).isEqualTo(new CreatedRestoreSlice(req))
                         .hasSameHashCodeAs(new CreatedRestoreSlice(req));

        CreateSliceRequestPayload differentReq = new CreateSliceRequestPayload("newId", 0, "bucket", "key", "checksum",
                                                                               BigInteger.ZERO, BigInteger.valueOf(2L), 234L, 123L);
        assertThat(slice).isNotEqualTo(new CreatedRestoreSlice(differentReq))
                         .doesNotHaveSameHashCodeAs(new CreatedRestoreSlice(differentReq));
    }
}
