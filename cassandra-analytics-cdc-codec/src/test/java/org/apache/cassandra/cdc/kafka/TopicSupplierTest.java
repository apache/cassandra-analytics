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
        assertThat(TopicSupplier.staticTopicSupplier("org.apache.cassandra.topicName").topic(event1))
        .as("Static topic supplier should return configured topic name")
        .isEqualTo("org.apache.cassandra.topicName");
        assertThat(TopicSupplier.staticTopicSupplier("org.apache.cassandra.analytics.topicName").topic(event1))
        .as("Static topic supplier should return configured topic name with prefix")
        .isEqualTo("org.apache.cassandra.analytics.topicName");
        assertThat(TopicSupplier.keyspaceSupplier("org.apache.cassandra.analytics.%s").topic(event1))
        .as("Keyspace supplier should format topic with keyspace")
        .isEqualTo("org.apache.cassandra.analytics.ks");
        assertThat(TopicSupplier.keyspaceSupplier("org.apache.cassandra.analytics.%s").topic(event2))
        .as("Keyspace supplier should format topic with different keyspace")
        .isEqualTo("org.apache.cassandra.analytics.test_ks");
        assertThat(TopicSupplier.keyspaceTableSupplier("org.apache.cassandra.analytics.%s.%s").topic(event1))
        .as("Keyspace-table supplier should format topic with keyspace and table")
        .isEqualTo("org.apache.cassandra.analytics.ks.tb");
        assertThat(TopicSupplier.keyspaceTableSupplier("org.apache.cassandra.analytics.%s.%s").topic(event2))
        .as("Keyspace-table supplier should format topic with different keyspace and table")
        .isEqualTo("org.apache.cassandra.analytics.test_ks.test_tb");
        assertThat(TopicSupplier.tableSupplier("org.apache.cassandra.analytics.%s").topic(event1))
        .as("Table supplier should format topic with table name")
        .isEqualTo("org.apache.cassandra.analytics.tb");
        assertThat(TopicSupplier.tableSupplier("org.apache.cassandra.analytics.%s").topic(event2))
        .as("Table supplier should format topic with different table name")
        .isEqualTo("org.apache.cassandra.analytics.test_tb");
        assertThat(TopicSupplier.mapSupplier("{\"ks.tb\": \"org.apache.cassandra.analytics.test_tb\"}").topic(event1))
        .as("Map supplier should return mapped topic name")
        .isEqualTo("org.apache.cassandra.analytics.test_tb");
    }
}
