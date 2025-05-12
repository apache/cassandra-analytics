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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.SidecarInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract class that provides a set of base unit tests for the {@link SidecarInstance} interface
 */
public abstract class SidecarInstanceTest
{
    protected abstract SidecarInstance newInstance(String hostname, int port);

    @Test
    public void failsWithInvalidPortNumber()
    {
        // Should use Parameterized instead
        int[] invalidPortNumbers = {-1, 0, 65536 };

        for (int invalidPortNumber : invalidPortNumbers)
        {
            try
            {
                newInstance(null, invalidPortNumber);
                fail("Expected to throw AssertionError when port is invalid");
            }
            catch (IllegalArgumentException illegalArgumentException)
            {
                assertEquals("Invalid port number for the Sidecar service: " + invalidPortNumber,
                             illegalArgumentException.getMessage());
            }
        }
    }

    @Test
    public void failsWithNullHostname()
    {
        try
        {
            newInstance(null, 8080);
            fail("Expected to throw NullPointerException when hostname is null");
        }
        catch (NullPointerException npe)
        {
            assertEquals("The Sidecar hostname must be non-null", npe.getMessage());
        }
    }

    @Test
    public void testConstructorWithValidParameters()
    {
        SidecarInstance instance1 = newInstance("localhost", 8080);
        assertNotNull(instance1);
        assertEquals(8080, instance1.port());
        assertEquals("localhost", instance1.hostname());

        SidecarInstance instance2 = newInstance("127.0.0.1", 1234);
        assertNotNull(instance2);
        assertEquals(1234, instance2.port());
        assertEquals("127.0.0.1", instance2.hostname());
    }
}
