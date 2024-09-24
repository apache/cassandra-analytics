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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.jetbrains.annotations.NotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RingInstanceTest
{
    public static RingInstance instance(BigInteger token, String nodeName, String datacenter)
    {
        return instance(token, nodeName, datacenter, null);
    }

    public static RingInstance instance(BigInteger token, String nodeName, String datacenter, String clusterId)
    {
        return new RingInstance(new RingEntry.Builder()
                                .datacenter(datacenter)
                                .port(7000)
                                .address(nodeName)
                                .status("UP")
                                .state("NORMAL")
                                .token(token.toString())
                                .fqdn(nodeName)
                                .rack("rack")
                                .owns("")
                                .load("")
                                .hostId("")
                                .build(),
                                clusterId);
    }

    @Test
    public void testEquals()
    {
        RingInstance instance1 = mockRingInstance();
        RingInstance instance2 = mockRingInstance();
        assertEquals(instance1, instance2);
    }

    @Test
    public void testHashCode()
    {
        RingInstance instance1 = mockRingInstance();
        RingInstance instance2 = mockRingInstance();
        assertEquals(instance1.hashCode(), instance2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeIgnoreNonCriticalFields()
    {
        RingEntry.Builder builder = mockRingEntryBuilder();
        // the fields chained in the builder below are not considered for equality check
        RingInstance instance1 = new RingInstance(builder
                                                  .status("1")
                                                  .state("1")
                                                  .load("1")
                                                  .hostId("1")
                                                  .owns("1")
                                                  .build());
        RingInstance instance2 = new RingInstance(builder
                                                  .status("2")
                                                  .state("2")
                                                  .load("2")
                                                  .hostId("2")
                                                  .owns("2")
                                                  .build());
        assertEquals(instance1, instance2);
        assertEquals(instance1.hashCode(), instance2.hashCode());
    }

    @Test
    public void testEqualsAndHashcodeConsidersClusterId()
    {
        RingEntry ringEntry = mockRingEntry();
        RingInstance c1i1 = new RingInstance(ringEntry, "cluster1");
        RingInstance c1i2 = new RingInstance(ringEntry, "cluster1");
        RingInstance c2i1 = new RingInstance(ringEntry, "cluster2");

        assertEquals(c1i1, c1i2);
        assertEquals(c1i1.hashCode(), c1i2.hashCode());

        assertNotEquals(c1i1, c2i1);
        assertNotEquals(c1i1.hashCode(), c2i1.hashCode());
    }

    @Test
    public void testHasClusterId()
    {
        RingEntry ringEntry = mockRingEntry();
        RingInstance instance = new RingInstance(ringEntry);
        assertFalse(instance.hasClusterId());

        RingInstance instanceWithClusterId = new RingInstance(ringEntry, "cluster1");
        assertTrue(instanceWithClusterId.hasClusterId());
        assertEquals("cluster1", instanceWithClusterId.clusterId());
    }

    @Test
    public void multiMapWorksWithRingInstances()
    {
        RingEntry ringEntry = mockRingEntry();
        RingInstance instance1 = new RingInstance(ringEntry);
        RingInstance instance2 = new RingInstance(ringEntry);
        byte[] buffer;

        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(instance2);
            out.close();
            buffer = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
            ObjectInputStream in = new ObjectInputStream(bais);
            instance2 = (RingInstance) in.readObject();
            in.close();
        }
        catch (final IOException | ClassNotFoundException exception)
        {
            throw new RuntimeException(exception);
        }
        Multimap<RingInstance, String> initialErrorMap = ArrayListMultimap.create();
        initialErrorMap.put(instance1, "Failure 1");
        Multimap<RingInstance, String> newErrorMap = ArrayListMultimap.create(initialErrorMap);
        newErrorMap.put(instance2, "Failure2");

        assertEquals(1, newErrorMap.keySet().size());
    }



    @NotNull
    private static RingEntry mockRingEntry()
    {
        return mockRingEntryBuilder().build();
    }

    @NotNull
    private static RingEntry.Builder mockRingEntryBuilder()
    {
        return new RingEntry.Builder()
               .datacenter("DATACENTER1")
               .address("127.0.0.1")
               .port(0)
               .rack("Rack")
               .status("UP")
               .state("NORMAL")
               .load("0")
               .token("0")
               .fqdn("DATACENTER1-i1")
               .hostId("")
               .owns("");
    }

    @NotNull
    private static RingInstance mockRingInstance()
    {
        return new RingInstance(mockRingEntry());
    }
}
