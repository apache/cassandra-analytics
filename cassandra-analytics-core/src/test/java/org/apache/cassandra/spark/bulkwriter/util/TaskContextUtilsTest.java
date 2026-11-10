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

package org.apache.cassandra.spark.bulkwriter.util;

import org.junit.jupiter.api.Test;

import org.apache.spark.TaskContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskContextUtilsTest
{
    @Test
    void testAttemptNumberRoundTripsThroughStreamSessionId()
    {
        TaskContext taskContext = mock(TaskContext.class);
        when(taskContext.partitionId()).thenReturn(13);
        when(taskContext.attemptNumber()).thenReturn(2);

        String sessionId = TaskContextUtils.createStreamSessionId(taskContext);
        assertThat(TaskContextUtils.attemptNumberFromStreamSessionId(sessionId)).isEqualTo(2);
    }

    @Test
    void testAttemptNumberDefaultsToZeroForUnrecognizedIds()
    {
        assertThat(TaskContextUtils.attemptNumberFromStreamSessionId(null)).isZero();
        assertThat(TaskContextUtils.attemptNumberFromStreamSessionId("")).isZero();
        assertThat(TaskContextUtils.attemptNumberFromStreamSessionId("sessionId")).isZero();
        assertThat(TaskContextUtils.attemptNumberFromStreamSessionId("sessionId-retry")).isZero();
        assertThat(TaskContextUtils.attemptNumberFromStreamSessionId("0-notANumber-uuid")).isZero();
    }
}
