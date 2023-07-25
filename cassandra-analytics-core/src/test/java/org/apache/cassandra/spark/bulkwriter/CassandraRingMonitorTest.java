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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraRingMonitorTest
{
    private int changeCount = 0;
    private CassandraRing<RingInstance> ring;
    private MockScheduledExecutorService executorService;

    @BeforeEach
    public void setup()
    {
        changeCount = 0;
        ring = buildRing(0);
        executorService = new MockScheduledExecutorService();
    }

    @Test
    public void noRingChange() throws Exception
    {
        MockScheduledExecutorService executorService = new MockScheduledExecutorService();
        CassandraRingMonitor crm = new CassandraRingMonitor(() -> ring,
                                                            event -> changeCount++,
                                                            1,
                                                            TimeUnit.SECONDS,
                                                            executorService);
        // Make no changes to the ring and call again
        executorService.runCommand();
        assertEquals(0, changeCount);
        assertFalse(crm.getRingChanged());
}

    @Test
    public void ringChanged() throws Exception
    {
        CassandraRingMonitor crm = new CassandraRingMonitor(() -> ring,
                                                            event -> changeCount++,
                                                            1,
                                                            TimeUnit.SECONDS,
                                                            executorService);
        // Make no changes to the ring and call again
        executorService.runCommand();
        assertEquals(0, changeCount);
        assertFalse(crm.getRingChanged());
        // Change the ring
        ring = buildRing(1);
        // Run the ring check again
        executorService.runCommand();
        assertEquals(1, changeCount);
        assertTrue(crm.getRingChanged());
        assertTrue(executorService.isStopped());
    }

    @Test
    public void stopShutsDownExecutor() throws Exception
    {
        CassandraRingMonitor crm = new CassandraRingMonitor(() -> ring,
                                                            event -> changeCount++,
                                                            1,
                                                            TimeUnit.SECONDS,
                                                            executorService);
        crm.stop();
        assertTrue(executorService.isStopped());
    }

    @Test
    public void passesTimeUnitCorrectly() throws Exception
    {
        CassandraRingMonitor crm = new CassandraRingMonitor(() -> ring,
                                                            event -> changeCount++,
                                                            10,
                                                            TimeUnit.HOURS,
                                                            executorService);
        crm.stop();
        assertEquals(10, executorService.getPeriod());
        assertEquals(TimeUnit.HOURS, executorService.getTimeUnit());
    }

    @Test
    public void multipleRingChangedFireOnceFalse() throws Exception
    {
        CassandraRingMonitor crm = new CassandraRingMonitor(() -> ring,
                                                            event -> changeCount++,
                                                            1,
                                                            TimeUnit.SECONDS,
                                                            executorService);
        // Make no changes to the ring and call again
        executorService.runCommand();
        assertEquals(0, changeCount);
        assertFalse(crm.getRingChanged());
        // Change the ring
        ring = buildRing(1);
        // Run the ring check again & make sure we increment change count
        executorService.runCommand();
        assertEquals(1, changeCount);
        assertTrue(crm.getRingChanged());
        // Make sure another check does not fire again, reset local state and change ring again
        ring = buildRing(2);
        executorService.runCommand();
        assertEquals(2, changeCount);
        assertTrue(crm.getRingChanged());
    }

    private CassandraRing<RingInstance> buildRing(int initialToken)
    {
        return RingUtils.buildRing(initialToken, "DEV", "test", 3);
    }
}
