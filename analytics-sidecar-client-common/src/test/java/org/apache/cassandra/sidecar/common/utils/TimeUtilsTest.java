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

package org.apache.cassandra.sidecar.common.utils;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TimeUtils} class
 */
public class TimeUtilsTest
{
    @Test
    public void testRandomDurationWithMinimumAboveMaximum()
    {
        assertThatThrownBy(() -> TimeUtils.randomDuration(Duration.ofSeconds(2L), Duration.ofSeconds(1L)))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testRandomDurationWithMinimumEqualToMaximum()
    {
        assertThat(TimeUtils.randomDuration(Duration.ofSeconds(1L), Duration.ofSeconds(1L)))
        .isEqualTo(Duration.ofSeconds(1L));
    }

    @Test
    public void testRandomDurationWithMinimumBelowMaximum()
    {
        for (int test = 0; test < 3600; test++)
        {
            Duration minimum = Duration.ofSeconds(test);
            Duration maximum = Duration.ofSeconds(test + 1);
            Duration random = TimeUtils.randomDuration(minimum, maximum);

            assertThat(minimum).isLessThanOrEqualTo(random);
            assertThat(maximum).isGreaterThanOrEqualTo(random);
        }
    }
}
