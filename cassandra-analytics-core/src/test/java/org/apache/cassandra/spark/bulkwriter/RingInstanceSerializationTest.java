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

import org.junit.Test;

import org.apache.cassandra.sidecar.common.data.RingEntry;

import static org.apache.cassandra.spark.utils.SerializationUtils.deserialize;
import static org.apache.cassandra.spark.utils.SerializationUtils.serialize;
import static org.junit.Assert.assertEquals;

public class RingInstanceSerializationTest
{
    @Test
    public void testRingSerializesCorrectly()
    {
        int dcOffset = 0;
        String dataCenter = "DC1";
        int index = 0;
        int initialToken = 0;
        RingInstance ring = new RingInstance(new RingEntry.Builder()
                                             .address("127.0." + dcOffset + "." + index)
                                             .port(7000)
                                             .datacenter(dataCenter)
                                             .load("0")
                                             .token(Integer.toString(initialToken + dcOffset + 100 * index))
                                             .fqdn(dataCenter + "-i" + index)
                                             .rack("Rack")
                                             .hostId("")
                                             .status("UP")
                                             .state("NORMAL")
                                             .owns("")
                                             .build());

        byte[] bytes = serialize(ring);
        RingInstance deserialized = deserialize(bytes, RingInstance.class);
        assertEquals(ring, deserialized);
    }
}
