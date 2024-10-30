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

package org.apache.cassandra.cdc.msg.jdk;

import java.util.Date;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.spark.data.types.BigInt;
import org.apache.cassandra.spark.data.types.Text;
import org.apache.cassandra.spark.data.types.Timestamp;
import org.apache.cassandra.spark.data.types.UUID;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CdcMessageTests
{
    @Test
    public void testCdcMessage()
    {
        long colA = (long) BigInt.INSTANCE.randomValue(1024);
        java.util.UUID colB = java.util.UUID.randomUUID();
        String colC = Text.INSTANCE.randomValue(1024).toString();
        Date colD = (Date) Timestamp.INSTANCE.randomValue(1024);
        long now = TimeUtils.nowMicros();

        CdcMessage msg = new CdcMessage(
        "ks", "tb",
        ImmutableList.of(new Column("a", BigInt.INSTANCE, colA)),
        ImmutableList.of(new Column("b", UUID.INSTANCE, colB)),
        ImmutableList.of(),
        ImmutableList.of(new Column("c", Text.INSTANCE, colC), new Column("d", Timestamp.INSTANCE, colD)),
        now,
        CdcEvent.Kind.INSERT,
        ImmutableList.of(), null, null
        );
        assertEquals(4, msg.allColumns().size());
        assertEquals("{\"operation\": INSERT, " +
                     "\"lastModifiedTimestamp\": " + now +
                     ", \"a\": \"" + colA +
                     "\", \"b\": \"" + colB +
                     "\", \"c\": \"" + colC +
                     "\", \"d\": \"" + colD + "\"}",
                     msg.toString());
    }
}
