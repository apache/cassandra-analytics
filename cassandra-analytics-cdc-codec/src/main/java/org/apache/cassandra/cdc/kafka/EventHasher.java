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

package org.apache.cassandra.cdc.kafka;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.Value;
import org.apache.kafka.common.utils.Utils;

public interface EventHasher
{
    EventHasher MURMUR2 = event -> {
        ByteBuffer[] values = event.getPartitionKeys()
                                   .stream()
                                   .map(Value::getValue)
                                   .toArray(ByteBuffer[]::new);

        byte[] ar = new byte[Arrays.stream(values).mapToInt(Buffer::remaining).sum()];
        int pos = 0;
        for (ByteBuffer buf : values)
        {
            final int len = buf.remaining();
            buf.get(ar, pos, len);
            pos += len;
        }

        return Integer.toHexString(Utils.murmur2(ar));
    };


    String hashEvent(CdcEvent event);
}
