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

package org.apache.cassandra.clients;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;

import static org.apache.cassandra.spark.utils.SerializationUtils.kryoDeserialize;
import static org.apache.cassandra.spark.utils.SerializationUtils.kryoSerialize;
import static org.apache.cassandra.spark.utils.SerializationUtils.register;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for the {@link SidecarInstanceImpl} class
 */
public class SidecarInstanceImplTest extends SidecarInstanceTest
{
    @BeforeAll
    public static void setupKryo()
    {
        register(SidecarInstanceImpl.class, new SidecarInstanceSerializer());
    }

    @Override
    protected SidecarInstance newInstance(String hostname, int port)
    {
        return new SidecarInstanceImpl(hostname, port);
    }

    @Test
    public void testKryoSerDe()
    {
        SidecarInstance instance = newInstance("localhost", 9043);
        Output out = kryoSerialize(instance);
        SidecarInstance deserialized = kryoDeserialize(out, SidecarInstanceImpl.class);
        assertNotNull(deserialized);
        assertEquals("localhost", deserialized.hostname());
        assertEquals(9043, deserialized.port());
    }
}
