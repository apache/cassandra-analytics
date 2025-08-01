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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.msg.CdcEventBuilder;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicSupplierTest
{
    @Test
    public void testTopicSupplier()
    {
        CdcEvent event1 = CdcEventBuilder.of(CdcEvent.Kind.INSERT, "ks", "tb").build();
        CdcEvent event2 = CdcEventBuilder.of(CdcEvent.Kind.UPDATE, "test_ks", "test_tb").build();
        assertThat(TopicSupplier.staticTopicSupplier("org.apple.amp.topicName").topic(event1))
        .as("Static topic supplier should return configured topic name")
        .isEqualTo("org.apple.amp.topicName");
        assertThat(TopicSupplier.staticTopicSupplier("org.apple.amp.itms5.topicName").topic(event1))
        .as("Static topic supplier should return configured topic name with prefix")
        .isEqualTo("org.apple.amp.itms5.topicName");
        assertThat(TopicSupplier.keyspaceSupplier("org.apple.amp.itms5.%s").topic(event1))
        .as("Keyspace supplier should format topic with keyspace")
        .isEqualTo("org.apple.amp.itms5.ks");
        assertThat(TopicSupplier.keyspaceSupplier("org.apple.amp.itms5.%s").topic(event2))
        .as("Keyspace supplier should format topic with different keyspace")
        .isEqualTo("org.apple.amp.itms5.test_ks");
        assertThat(TopicSupplier.keyspaceTableSupplier("org.apple.amp.itms5.%s.%s").topic(event1))
        .as("Keyspace-table supplier should format topic with keyspace and table")
        .isEqualTo("org.apple.amp.itms5.ks.tb");
        assertThat(TopicSupplier.keyspaceTableSupplier("org.apple.amp.itms5.%s.%s").topic(event2))
        .as("Keyspace-table supplier should format topic with different keyspace and table")
        .isEqualTo("org.apple.amp.itms5.test_ks.test_tb");
        assertThat(TopicSupplier.tableSupplier("org.apple.amp.itms5.%s").topic(event1))
        .as("Table supplier should format topic with table name")
        .isEqualTo("org.apple.amp.itms5.tb");
        assertThat(TopicSupplier.tableSupplier("org.apple.amp.itms5.%s").topic(event2))
        .as("Table supplier should format topic with different table name")
        .isEqualTo("org.apple.amp.itms5.test_tb");
        assertThat(TopicSupplier.mapSupplier("{\"ks.tb\": \"org.apple.amp.itms5.test_tb\"}").topic(event1))
        .as("Map supplier should return mapped topic name")
        .isEqualTo("org.apple.amp.itms5.test_tb");
    }
}
