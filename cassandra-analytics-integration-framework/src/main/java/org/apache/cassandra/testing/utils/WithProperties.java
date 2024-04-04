/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.testing.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parts of this class are copied from Cassandra's {@code org.apache.cassandra.distributed.shared.WithProperties}
 * class.
 */
public class WithProperties implements AutoCloseable
{
    private final List<Runnable> rollback = new ArrayList<>();

    public WithProperties with(String... kvs)
    {
        assert kvs.length % 2 == 0 : "Input must have an even amount of inputs but given " + kvs.length;
        for (int i = 0; i <= kvs.length - 2; i = i + 2)
        {
            with(kvs[i], kvs[i + 1]);
        }
        return this;
    }

    private void with(String key, String value)
    {
        String previous = System.setProperty(key, value);
        rollback.add(previous == null ? () -> System.clearProperty(key) : () -> System.setProperty(key, previous));
    }

    @Override
    public void close()
    {
        Collections.reverse(rollback);
        rollback.forEach(Runnable::run);
        rollback.clear();
    }
}
